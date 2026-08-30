//! Lazy virtual tree.
//!
//! A directory's children only exist in memory once something actually
//! reads that directory. Mounting a drive with millions of files is
//! therefore instant: the cost is paid per directory visited, never up
//! front for the whole tree. A directory listing is cached for
//! [`LISTING_TTL`], and can be forgotten early via [`VfsTree::invalidate_path`]
//! once phase 4 wires up SSE.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use cloudreve_api::api::explorer::ExplorerApiExt;
use cloudreve_api::models::common::ListAllRes;
use cloudreve_api::models::explorer::{file_type, ListResponse};
use tokio::sync::RwLock;

/// How long a directory listing is trusted before `readdir` will hit the
/// network again on its own. Short enough that a stale view of a
/// server-side change (not yet caught by SSE/invalidation) self-heals fast;
/// long enough that a Finder-style burst of readdir calls on the same
/// directory costs exactly one HTTP request.
pub const LISTING_TTL: Duration = Duration::from_secs(5);

/// Opaque handle to a node in the tree. Allocated once per (parent, name)
/// pair and stable for the lifetime of the `VfsTree`, so frontends (NFS on
/// macOS, FUSE on Linux) can cache by id across repeated lookups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);

/// The tree always has exactly one root, allocated up front — before any
/// listing happens — so `VfsTree::root()` never needs to be fallible.
const ROOT: NodeId = NodeId(1);

/// Everything a caller needs to know about a node without listing it.
#[derive(Debug, Clone)]
pub struct NodeAttr {
    pub name: String,
    /// Full `cloudreve://…` uri, exactly as returned by the server's `path`
    /// field (or, for the root, the tree's configured remote base).
    pub remote_path: String,
    pub size: u64,
    pub mtime_secs: i64,
    pub is_dir: bool,
    /// Empty for directories: Cloudreve only versions file content.
    pub etag: String,
}

/// Page size for listing a single directory. Unlike `list_remote_recursive`'s
/// whole-drive walk, there is no cross-directory page-size negotiation to do
/// here: each call targets exactly one directory and fully drains it before
/// returning, so any reasonable size works.
const PAGE_SIZE: i32 = 200;

/// All mutable tree state, behind one lock so a listing that allocates new
/// ids and one that only reads an already-cached directory can't observe
/// each other half-updated.
struct Inner {
    attrs: HashMap<NodeId, NodeAttr>,
    /// Present once a directory has been listed; a missing entry is exactly
    /// the "not read yet" signal `readdir`/`lookup` use to decide whether a
    /// network round-trip is needed.
    children: HashMap<NodeId, Vec<NodeId>>,
    /// When each directory's `children` entry was last populated. Consulted
    /// alongside `children`: a directory can be present but stale, in which
    /// case it is treated the same as "not read yet".
    listed_at: HashMap<NodeId, Instant>,
    /// Ids assigned to (parent, name) pairs, kept even if the directory is
    /// ever re-listed, so a node's id never changes for the tree's lifetime.
    known_children: HashMap<(NodeId, String), NodeId>,
    next_id: u64,
}

pub struct VfsTree {
    client: Arc<cloudreve_api::Client>,
    inner: RwLock<Inner>,
}

impl VfsTree {
    pub fn new(client: Arc<cloudreve_api::Client>, remote_base: String) -> Self {
        let mut attrs = HashMap::new();
        attrs.insert(
            ROOT,
            NodeAttr {
                name: String::new(),
                remote_path: remote_base,
                size: 0,
                mtime_secs: 0,
                is_dir: true,
                etag: String::new(),
            },
        );
        Self {
            client,
            inner: RwLock::new(Inner {
                attrs,
                children: HashMap::new(),
                listed_at: HashMap::new(),
                known_children: HashMap::new(),
                next_id: 2, // NodeId(1) is the root.
            }),
        }
    }

    pub fn root(&self) -> NodeId {
        ROOT
    }

    /// Lists a directory, fetching it from the server on first read and
    /// serving every later call from the cache built by that first read.
    pub async fn readdir(&self, dir: NodeId) -> Result<Vec<(NodeId, NodeAttr)>> {
        self.ensure_listed(dir).await?;
        let inner = self.inner.read().await;
        let ids = inner.children.get(&dir).cloned().unwrap_or_default();
        Ok(ids
            .into_iter()
            .filter_map(|id| inner.attrs.get(&id).cloned().map(|attr| (id, attr)))
            .collect())
    }

    /// Resolves one child by name, listing the parent first if needed.
    pub async fn lookup(&self, parent: NodeId, name: &str) -> Result<Option<(NodeId, NodeAttr)>> {
        self.ensure_listed(parent).await?;
        let inner = self.inner.read().await;
        let Some(ids) = inner.children.get(&parent) else {
            return Ok(None);
        };
        Ok(ids.iter().find_map(|id| {
            inner
                .attrs
                .get(id)
                .filter(|attr| attr.name == name)
                .map(|attr| (*id, attr.clone()))
        }))
    }

    /// Reads a node's own attributes. Never triggers a listing: `node` must
    /// already be known, e.g. from an earlier `readdir`/`lookup`.
    pub async fn getattr(&self, node: NodeId) -> Result<Option<NodeAttr>> {
        Ok(self.inner.read().await.attrs.get(&node).cloned())
    }

    /// Fetches and caches a directory's children the first time it is read,
    /// and again whenever the cached copy has outlived [`LISTING_TTL`]. A
    /// directory that is already cached and still fresh returns immediately:
    /// this is the whole point of the tree being lazy — a burst of reads on
    /// the same directory costs at most one network round-trip.
    async fn ensure_listed(&self, dir: NodeId) -> Result<()> {
        {
            let inner = self.inner.read().await;
            let fresh = inner
                .listed_at
                .get(&dir)
                .is_some_and(|listed_at| listed_at.elapsed() < LISTING_TTL);
            if inner.children.contains_key(&dir) && fresh {
                return Ok(());
            }
        }

        let remote_path = {
            let inner = self.inner.read().await;
            inner
                .attrs
                .get(&dir)
                .context("readdir/lookup on an unknown node")?
                .remote_path
                .clone()
        };

        let mut files = Vec::new();
        let mut previous: Option<ListAllRes<ListResponse>> = None;
        loop {
            let mut page = self
                .client
                .list_files_all(previous.as_ref(), &remote_path, PAGE_SIZE)
                .await
                .context("failed to list remote directory")?;
            files.extend(std::mem::take(&mut page.res.files));
            let more = page.more;
            previous = Some(page);
            if !more {
                break;
            }
        }

        let mut inner = self.inner.write().await;
        // Another concurrent call for the same directory won the race while
        // we were awaiting the network and its result is still fresh:
        // nothing left to do.
        let fresh = inner
            .listed_at
            .get(&dir)
            .is_some_and(|listed_at| listed_at.elapsed() < LISTING_TTL);
        if inner.children.contains_key(&dir) && fresh {
            return Ok(());
        }

        // Names present in this fresh listing: anything else previously
        // known under `dir` is a ghost — deleted (or renamed) on the server
        // since the last listing — and gets pruned below once the new ids
        // are known, so `known_children`/`attrs` don't grow forever under
        // churn.
        let fresh_names: std::collections::HashSet<String> =
            files.iter().map(|f| f.name.clone()).collect();

        // Snapshot of `dir`'s previously known (name, id) children, taken
        // before anything below is mutated. Diffing this local list against
        // `fresh_names` costs O(children of dir) — never O(every
        // known_children pair in the whole tree) — keeping the module's
        // "cost is paid per directory visited" invariant intact even under
        // churn. Sourced from `children`, not a scan of `known_children`:
        // that is exactly why `invalidate_path` below only clears
        // `listed_at`, not `children` — this list must still be here when a
        // re-list was triggered by invalidation, not just by TTL expiry.
        let old_children: Vec<(String, NodeId)> = inner
            .children
            .get(&dir)
            .into_iter()
            .flatten()
            .filter_map(|id| inner.attrs.get(id).map(|attr| (attr.name.clone(), *id)))
            .collect();

        let mut child_ids = Vec::with_capacity(files.len());
        for f in files {
            let key = (dir, f.name.clone());
            let id = if let Some(&existing) = inner.known_children.get(&key) {
                existing
            } else {
                let id = NodeId(inner.next_id);
                inner.next_id += 1;
                inner.known_children.insert(key, id);
                id
            };
            let attr = NodeAttr {
                name: f.name,
                remote_path: f.path,
                size: f.size.max(0) as u64,
                mtime_secs: parse_rfc3339(&f.updated_at),
                is_dir: f.file_type == file_type::FOLDER,
                etag: f.primary_entity.unwrap_or_default(),
            };
            inner.attrs.insert(id, attr);
            child_ids.push(id);
        }

        // Prune ghosts: any previously known child of `dir` absent from the
        // fresh listing no longer exists remotely. Names still present
        // above keep their NodeId untouched — only vanished names are
        // removed here. When a pruned ghost is itself a directory, its
        // whole in-memory subtree goes with it (see `remove_subtree`), or
        // its descendants' `children`/`listed_at`/`known_children` entries
        // would orphan forever, unreachable from the tree yet still
        // resident.
        for (name, ghost_id) in old_children {
            if !fresh_names.contains(&name) {
                inner.known_children.remove(&(dir, name));
                remove_subtree(&mut inner, ghost_id);
            }
        }

        inner.children.insert(dir, child_ids);
        inner.listed_at.insert(dir, Instant::now());
        Ok(())
    }

    /// Forget the cached listing containing this remote path (and the
    /// entry's attributes), so the next readdir/lookup refetches. Called by
    /// the SSE hookup in phase 4 and after writes in phase 2.
    pub async fn invalidate_path(&self, remote_path: &str) {
        let Some((parent_path, _name)) = remote_path.rsplit_once('/') else {
            return;
        };

        let mut inner = self.inner.write().await;

        // The directory that listed this entry: only its `listed_at` stamp
        // is dropped, not `children` itself. Clearing `listed_at` alone
        // already makes `ensure_listed`'s freshness check fail, forcing the
        // next readdir/lookup back to the network — while leaving the last-
        // known child list in place for `ensure_listed` to diff the fresh
        // listing against when pruning ghosts (see its `old_children`
        // snapshot). Removing `children` here too would erase that
        // snapshot before the re-list ever runs.
        if let Some(dir_id) = inner
            .attrs
            .iter()
            .find(|(_, attr)| attr.is_dir && attr.remote_path == parent_path)
            .map(|(id, _)| *id)
        {
            inner.listed_at.remove(&dir_id);
        }

        // The entry's own attributes, if already known: drop them too, so a
        // direct getattr can't serve a stale copy before the parent is
        // relisted (getattr never triggers a listing on its own).
        if let Some(entry_id) = inner
            .attrs
            .iter()
            .find(|(_, attr)| attr.remote_path == remote_path)
            .map(|(id, _)| *id)
        {
            inner.attrs.remove(&entry_id);
        }
    }

    /// Inserts a local-only entry (created via `Vfs::create`, not yet
    /// confirmed by any server listing) as a child of `parent`, so it is
    /// immediately visible to lookups/readdir once the facade overlays it
    /// (Task 7 — see `Vfs::readdir`/`lookup`). Reuses `known_children`'s
    /// (parent, name) allocation scheme so the id stays stable if the
    /// server later lists a file of the same name (e.g. once the pending
    /// upload completes and the directory is re-listed): same id, no
    /// frontend-visible churn.
    ///
    /// Deliberately does NOT touch `children`/`listed_at`: those drive
    /// `ensure_listed`'s ghost-pruning diff, which must never see (and
    /// therefore never prune) an entry that has no remote counterpart at
    /// all — a purely local entry surviving indefinitely across re-lists is
    /// the whole point.
    #[doc(hidden)]
    pub async fn insert_local_entry(&self, parent: NodeId, name: &str) -> Result<NodeId> {
        let mut inner = self.inner.write().await;
        let parent_path = inner
            .attrs
            .get(&parent)
            .context("insert_local_entry: unknown parent")?
            .remote_path
            .clone();

        let key = (parent, name.to_string());
        let id = if let Some(&existing) = inner.known_children.get(&key) {
            existing
        } else {
            let id = NodeId(inner.next_id);
            inner.next_id += 1;
            inner.known_children.insert(key, id);
            id
        };
        inner.attrs.insert(
            id,
            NodeAttr {
                name: name.to_string(),
                remote_path: format!("{parent_path}/{name}"),
                size: 0,
                // The facade overlays the draft's real mtime (D3); this is
                // just the placeholder for an entry with no draft yet.
                mtime_secs: 0,
                is_dir: false,
                etag: String::new(),
            },
        );
        Ok(id)
    }

    /// Every child of `parent` known to the tree by (parent, name)
    /// allocation, regardless of whether it is part of `parent`'s current
    /// listing — the facade uses this (Task 7) to overlay locally-created
    /// entries (via `insert_local_entry`) onto `readdir`'s server-backed
    /// result without ever mutating `children`/`listed_at` itself. A ghost
    /// pruned by `ensure_listed` is gone from `known_children` too (see
    /// `remove_subtree`), so this never resurrects a deleted entry.
    #[doc(hidden)]
    pub async fn known_children_of(&self, parent: NodeId) -> Vec<(NodeId, NodeAttr)> {
        let inner = self.inner.read().await;
        inner
            .known_children
            .iter()
            .filter(|((p, _), _)| *p == parent)
            .filter_map(|(_, id)| inner.attrs.get(id).map(|attr| (*id, attr.clone())))
            .collect()
    }

    /// Force a directory's cached listing to be treated as expired,
    /// regardless of when it was actually populated. Test-only: production
    /// code relies on wall-clock TTL expiry, not this shortcut. Currently
    /// unused — neither of this task's tests needs it (freshness relies on
    /// TTL not expiring within a fast test; invalidation goes through
    /// `invalidate_path`) — kept for the next test that needs to force
    /// expiry without waiting out `LISTING_TTL`.
    #[cfg(test)]
    #[allow(dead_code)]
    async fn force_expire(&self, dir: NodeId) {
        let mut inner = self.inner.write().await;
        if let Some(listed_at) = inner.listed_at.get_mut(&dir) {
            *listed_at = Instant::now() - LISTING_TTL - Duration::from_secs(1);
        }
    }
}

/// Removes a pruned node's own bookkeeping and, when it was a directory,
/// recurses into every already-listed descendant first — so a ghost
/// directory takes its whole in-memory subtree down with it instead of
/// leaving `children`/`listed_at`/`known_children` entries under it
/// orphaned forever. Each `known_children` removal is a direct key lookup
/// (parent id + name), never a scan of the map: cost is proportional to the
/// size of the subtree actually resident in memory, not to the whole tree.
fn remove_subtree(inner: &mut Inner, id: NodeId) {
    if let Some(child_ids) = inner.children.remove(&id) {
        for child_id in child_ids {
            if let Some(name) = inner.attrs.get(&child_id).map(|attr| attr.name.clone()) {
                inner.known_children.remove(&(id, name));
            }
            remove_subtree(inner, child_id);
        }
    }
    inner.listed_at.remove(&id);
    inner.attrs.remove(&id);
}

/// Parses a server timestamp the same way `drive::sync` does: on failure —
/// which should not happen against a well-behaved server — degrade to
/// epoch 0 rather than aborting the whole listing over one bad clock string.
fn parse_rfc3339(s: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.timestamp())
        .unwrap_or(0)
}
