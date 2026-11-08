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
/// macOS, FUSE on Linux) can cache by id across repeated lookups. A rename
/// carries it to the new (parent, name) (see `VfsTree::rename_entry`).
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
    /// Sub-second part of the mtime. Always 0 for a file (the server's
    /// clock has whole seconds). A directory's moves when its listing
    /// changes, see [`bump_dir_mtime`].
    pub mtime_nsecs: u32,
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
    /// ever re-listed, so a node's id never changes for the tree's lifetime
    /// — a rename re-keys the pair rather than minting a new id.
    known_children: HashMap<(NodeId, String), NodeId>,
    /// Files whose own path was invalidated (SSE, write-back), mapped to
    /// the directory that lists them. Their `attrs` entry is kept — the
    /// kernel keeps addressing the file by this NodeId — but the next
    /// `getattr` re-lists that directory before answering, so it never
    /// serves the stale copy either. Cleared by that re-list.
    stale_files: HashMap<NodeId, NodeId>,
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
                mtime_nsecs: 0,
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
                stale_files: HashMap::new(),
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

    /// Reads a node's own attributes. `node` must already be known, e.g.
    /// from an earlier `readdir`/`lookup`. Triggers a listing only for a
    /// file whose own path was invalidated (see `stale_files`): its parent
    /// is re-listed first, so the answer is the server's current view under
    /// the SAME NodeId — or `None` if the file is gone. A failed re-list
    /// (offline) falls back to the last known attributes rather than making
    /// a file the kernel holds open vanish.
    pub async fn getattr(&self, node: NodeId) -> Result<Option<NodeAttr>> {
        let stale_parent = self.inner.read().await.stale_files.get(&node).copied();
        if let Some(parent) = stale_parent {
            if let Err(err) = self.ensure_listed(parent).await {
                tracing::debug!(?err, "vfs tree: re-list for a stale file failed, serving last known attrs");
            }
        }
        Ok(self.inner.read().await.attrs.get(&node).cloned())
    }

    /// Re-lists `node` first if it is a directory that was listed before
    /// and whose listing is no longer fresh (invalidated, or past
    /// [`LISTING_TTL`]), so the attributes a frontend reports for it carry
    /// the server's current state — in particular a moved mtime when its
    /// children changed (see `bump_dir_mtime`), the kernel's only cue to
    /// drop what it cached for that directory. A directory never listed has
    /// nothing cached anywhere and costs nothing here; a fresh one is served
    /// from memory, so HTTP stays bounded by the listing TTL however often
    /// the kernel asks. A failed re-list (offline) keeps the last known
    /// attributes.
    pub async fn refresh_dir_if_stale(&self, node: NodeId) {
        let stale = {
            let inner = self.inner.read().await;
            let is_dir = inner.attrs.get(&node).is_some_and(|attr| attr.is_dir);
            let fresh = inner
                .listed_at
                .get(&node)
                .is_some_and(|listed_at| listed_at.elapsed() < LISTING_TTL);
            is_dir && inner.children.contains_key(&node) && !fresh
        };
        if stale {
            if let Err(err) = self.ensure_listed(node).await {
                tracing::debug!(?err, "vfs tree: re-list of a stale directory failed, serving last known attrs");
            }
        }
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

        // macOS metadata junk the server holds (another client uploaded a
        // `.DS_Store`, say) never enters the tree: it is not listed, not
        // resolvable by name, and never shadows the local-only overlay of
        // the same name (see `crate::junk`).
        files.retain(|f| crate::junk::classify(&f.name).is_none());

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

        // Whether the kernel may already hold a view of this listing — the
        // only case in which a change has to be signalled to it at all —
        // and whether this listing changed anything it could have cached
        // (a name, or a child's attributes READDIRPLUS handed out).
        let was_listed = inner.children.contains_key(&dir);
        let mut changed = false;

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
            let mut attr = NodeAttr {
                name: f.name,
                remote_path: f.path,
                size: f.size.max(0) as u64,
                mtime_secs: parse_rfc3339(&f.updated_at),
                mtime_nsecs: 0,
                is_dir: f.file_type == file_type::FOLDER,
                etag: f.primary_entity.unwrap_or_default(),
            };
            // Same content version (etag) as already known: keep the size
            // and mtime the kernel has already been told. They only change
            // when the content does — never flap on a re-list. This is what
            // keeps `record_upload`'s attrs (the bytes we just uploaded,
            // stamped with the draft's mtime) from being replaced by a
            // listing whose clock is older or that still lags the upload.
            if let Some(old) = inner.attrs.get(&id) {
                if !attr.is_dir && !old.is_dir && !attr.etag.is_empty() && old.etag == attr.etag {
                    attr.size = old.size;
                    attr.mtime_secs = old.mtime_secs;
                }
                // A directory's mtime is ours, not the server's (see
                // `bump_dir_mtime`): Cloudreve's `updated_at` for a folder
                // need not follow its children, so it can be older than
                // what the kernel was told. Never move it backwards.
                if attr.is_dir && old.is_dir && (old.mtime_secs, old.mtime_nsecs) > (attr.mtime_secs, 0) {
                    attr.mtime_secs = old.mtime_secs;
                    attr.mtime_nsecs = old.mtime_nsecs;
                }
                changed |= old.size != attr.size
                    || (old.mtime_secs, old.mtime_nsecs) != (attr.mtime_secs, attr.mtime_nsecs)
                    || old.is_dir != attr.is_dir
                    || old.etag != attr.etag;
            }
            inner.attrs.insert(id, attr);
            inner.stale_files.remove(&id);
            child_ids.push(id);
        }

        // A stale file of `dir` (see `stale_files`) absent from this
        // listing is gone remotely. Ghost-pruning below only covers
        // server-listed children; this also covers a local-only entry
        // (`insert_local_entry`) whose path was invalidated, e.g. renamed
        // away — dropped exactly as `invalidate_path` used to drop it.
        let vanished: Vec<NodeId> = inner
            .stale_files
            .iter()
            .filter(|(_, parent)| **parent == dir)
            .map(|(id, _)| *id)
            .collect();
        for id in vanished {
            inner.stale_files.remove(&id);
            if let Some(name) = inner.attrs.get(&id).map(|attr| attr.name.clone()) {
                inner.known_children.remove(&(dir, name));
            }
            remove_subtree(&mut inner, id);
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
                changed = true;
                inner.known_children.remove(&(dir, name));
                remove_subtree(&mut inner, ghost_id);
            }
        }

        // A name the previous listing did not have (a remote create or the
        // target of a remote rename) is a change too.
        if let Some(old_ids) = inner.children.get(&dir) {
            let old_ids: std::collections::HashSet<&NodeId> = old_ids.iter().collect();
            changed |= child_ids.iter().any(|id| !old_ids.contains(id));
        }
        if was_listed && changed {
            if let Some(attr) = inner.attrs.get_mut(&dir) {
                bump_dir_mtime(attr);
            }
        }

        inner.children.insert(dir, child_ids);
        inner.listed_at.insert(dir, Instant::now());
        Ok(())
    }

    /// Forget the cached listing containing this remote path (and the
    /// entry's attributes), so the next readdir/lookup refetches. Called by
    /// the SSE hookup in phase 4 and after writes in phase 2.
    ///
    /// Phase-4 obligation (carried from phase 2): when `remote_path` itself
    /// names an already-known DIRECTORY — not just one of its children, or
    /// its parent — that directory's OWN cached listing is cleared too (see
    /// the first block below). Before this existed, only the PARENT's
    /// `listed_at` entry for the directory was ever touched here; the
    /// directory's own `children`/`listed_at` survived untouched, so a
    /// direct readdir of that same directory within [`LISTING_TTL`] kept
    /// serving the stale list — `ensure_listed`'s freshness check saw
    /// `children` present and `listed_at` still fresh and returned
    /// immediately, never even reaching the point where the (harmlessly
    /// dropped) attrs entry would have mattered.
    pub async fn invalidate_path(&self, remote_path: &str) {
        let mut inner = self.inner.write().await;

        // The directory named by `remote_path` itself, if any is known:
        // its own listing is treated as expired, exactly like `LISTING_TTL`
        // running out early. Deliberately does NOT remove the directory's own
        // `NodeAttr` (unlike the file case at the bottom of this function):
        // `ensure_listed` needs `attrs.get(&dir).remote_path` to know what
        // to re-list, so the node must stay resolvable by its `NodeId`.
        // A directory that was actually deleted/renamed away, rather than
        // merely having its OWN contents change, is still cleaned up in
        // full (attrs included) the ordinary way: this call also forces the
        // PARENT to re-list (see the next block), and that re-list's
        // ghost-pruning (`ensure_listed`'s `old_children` diff) removes the
        // whole subtree once it actually runs. This also covers invalidating
        // the tree's own root path: the root has no parent for the next
        // block to find, but its own listing must still be forgotten.
        let self_dir_id = inner
            .attrs
            .iter()
            .find(|(_, attr)| attr.is_dir && attr.remote_path == remote_path)
            .map(|(id, _)| *id);
        // Only `listed_at` goes, exactly like the parent below: the last-
        // known `children` must survive for `ensure_listed` to diff the
        // fresh listing against — that diff prunes the ghosts AND decides
        // whether this directory's mtime moves (see `bump_dir_mtime`).
        if let Some(dir_id) = self_dir_id {
            inner.listed_at.remove(&dir_id);
        }

        let Some((parent_path, _name)) = remote_path.rsplit_once('/') else {
            return;
        };

        // The directory that listed this entry: only its `listed_at` stamp
        // is dropped, not `children` itself. Clearing `listed_at` alone
        // already makes `ensure_listed`'s freshness check fail, forcing the
        // next readdir/lookup back to the network — while leaving the last-
        // known child list in place for `ensure_listed` to diff the fresh
        // listing against when pruning ghosts (see its `old_children`
        // snapshot). Removing `children` here too would erase that
        // snapshot before the re-list ever runs.
        let parent_id = inner
            .attrs
            .iter()
            .find(|(_, attr)| attr.is_dir && attr.remote_path == parent_path)
            .map(|(id, _)| *id);
        if let Some(dir_id) = parent_id {
            inner.listed_at.remove(&dir_id);
        }

        // The entry's own attributes, if already known: a direct getattr
        // must not serve a stale copy before the parent is relisted. They
        // are NOT dropped, though (they used to be): the kernel keeps
        // addressing an open file by its NodeId without a fresh LOOKUP, so
        // a dropped entry made every GETATTR/READ/WRITE on it fail until
        // something happened to re-list the parent — a lost WRITE, and a
        // file that blinks out and back under the same fileid. Instead the
        // file is marked stale and `getattr` re-lists the parent first
        // (see `stale_files`). Skipped when `remote_path` was itself the
        // directory handled above: that node's own `NodeAttr` must survive
        // (see that block's doc).
        if self_dir_id.is_none() {
            if let Some(entry_id) = inner
                .attrs
                .iter()
                .find(|(_, attr)| attr.remote_path == remote_path)
                .map(|(id, _)| *id)
            {
                match parent_id {
                    Some(dir_id) => {
                        inner.stale_files.insert(entry_id, dir_id);
                    }
                    // No known parent to re-list it from: nothing could
                    // ever refresh it, so drop it as before.
                    None => {
                        inner.attrs.remove(&entry_id);
                    }
                }
            }
        }
    }

    /// The folder whose listing a change to `remote_path` affects — its
    /// parent, or the root itself when `remote_path` is the root — as a
    /// path relative to the root (`""` for the root), provided that folder
    /// has been listed: one never listed has nothing on screen to refresh.
    pub async fn listed_folder_of(&self, remote_path: &str) -> Option<String> {
        let inner = self.inner.read().await;
        let base = &inner.attrs.get(&ROOT)?.remote_path;
        let folder = if remote_path == base {
            remote_path
        } else {
            remote_path.rsplit_once('/')?.0
        };
        let listed = inner
            .attrs
            .iter()
            .any(|(id, attr)| attr.is_dir && attr.remote_path == folder && inner.children.contains_key(id));
        if !listed {
            return None;
        }
        if folder == base {
            return Some(String::new());
        }
        folder.strip_prefix(base.as_str())?.strip_prefix('/').map(str::to_string)
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
                mtime_nsecs: 0,
                is_dir: false,
                etag: String::new(),
            },
        );
        Ok(id)
    }

    /// A rename that already happened (on the server, or in the local-only
    /// draft/overlay state) carries the entry's identity along: the
    /// `NodeId` the kernel holds for `old_parent`/`old_name` now names
    /// `new_parent`/`new_name`, so an app still writing to it by fileid or
    /// inode (a downloader renaming `x.part`, an editor saving) keeps
    /// reaching the same file. A directory keeps its whole resident
    /// subtree: children are keyed by the directory's id, which does not
    /// change, and every descendant's `remote_path` is rewritten.
    ///
    /// An entry already known at the destination (rename over an existing
    /// file) is retired with its subtree: its id stops resolving rather
    /// than aliasing the file that replaced it.
    ///
    /// The moved id leaves `old_parent`'s listed `children` — or the next
    /// re-list of `old_parent` would prune it as a ghost under its new name
    /// — and joins `new_parent`'s if it was a listed (server) entry.
    pub async fn rename_entry(&self, old_parent: NodeId, old_name: &str, new_parent: NodeId, new_name: &str) {
        let mut inner = self.inner.write().await;
        let Some(new_parent_path) = inner.attrs.get(&new_parent).map(|a| a.remote_path.clone()) else {
            return;
        };
        let Some(id) = inner.known_children.remove(&(old_parent, old_name.to_string())) else {
            return;
        };
        if let Some(dest) = inner.known_children.remove(&(new_parent, new_name.to_string())) {
            if dest != id {
                if let Some(list) = inner.children.get_mut(&new_parent) {
                    list.retain(|c| *c != dest);
                }
                remove_subtree(&mut inner, dest);
            }
        }
        let was_listed = match inner.children.get_mut(&old_parent) {
            Some(list) => {
                let before = list.len();
                list.retain(|c| *c != id);
                list.len() != before
            }
            None => false,
        };
        // Only a server-listed entry joins the new parent's listing: a
        // local-only one (a draft never uploaded, a junk file) must stay
        // out of `children`, whose re-list diff would prune it as a ghost.
        if was_listed {
            if let Some(list) = inner.children.get_mut(&new_parent) {
                list.push(id);
            }
        }
        inner.known_children.insert((new_parent, new_name.to_string()), id);
        if let Some(parent) = inner.stale_files.get_mut(&id) {
            *parent = new_parent;
        }

        let new_path = format!("{new_parent_path}/{new_name}");
        let Some(attr) = inner.attrs.get_mut(&id) else { return };
        let old_path = std::mem::replace(&mut attr.remote_path, new_path.clone());
        attr.name = new_name.to_string();
        if attr.is_dir {
            let old_prefix = format!("{old_path}/");
            for attr in inner.attrs.values_mut() {
                if let Some(rest) = attr.remote_path.strip_prefix(&old_prefix) {
                    attr.remote_path = format!("{new_path}/{rest}");
                }
            }
        }
    }

    /// Drops a local-only entry made by `insert_local_entry` right away,
    /// without the parent re-list `invalidate_path` would cost: its id no
    /// longer resolves. For entries the server never lists (local-only
    /// junk, see `crate::junk`) — a server-listed entry would come back on
    /// the next listing anyway.
    #[doc(hidden)]
    pub async fn forget_local_entry(&self, parent: NodeId, name: &str) {
        let mut inner = self.inner.write().await;
        if let Some(id) = inner.known_children.remove(&(parent, name.to_string())) {
            inner.attrs.remove(&id);
            inner.stale_files.remove(&id);
        }
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

    /// Forces a fresh listing of `remote_path`'s parent directory and
    /// returns the server's current full attributes for it — `None` if the
    /// parent directory isn't known to the tree yet, or if the entry no
    /// longer exists there (deleted remotely since the caller's own last
    /// look). Deliberately works from a bare remote path alone, no
    /// `NodeId`: the write-back queue (Task 8) only ever holds paths —
    /// `DraftStore` never allocates or tracks node ids. Phase 4 task 3
    /// (R3): factored out of `refresh_etag` below so a caller that needs
    /// more than the etag (specifically, `is_dir` — is the uri colliding
    /// with a 40004 actually a file this could safely adopt, or a
    /// directory it must not) doesn't have to re-derive the same
    /// parent-lookup dance a second time.
    pub async fn refresh_attrs(&self, remote_path: &str) -> Result<Option<NodeAttr>> {
        let Some((parent_path, name)) = remote_path.rsplit_once('/') else {
            return Ok(None);
        };
        self.invalidate_path(remote_path).await;
        let parent_id = {
            let inner = self.inner.read().await;
            inner
                .attrs
                .iter()
                .find(|(_, attr)| attr.is_dir && attr.remote_path == parent_path)
                .map(|(id, _)| *id)
        };
        let Some(parent_id) = parent_id else { return Ok(None) };
        Ok(self.lookup(parent_id, name).await?.map(|(_, attr)| attr))
    }

    /// Write-back (NFS hardening): a draft of `remote_path` was just
    /// uploaded — the server now holds exactly `size` bytes, last written
    /// locally at `mtime_secs`. Records those as the file's attributes, so
    /// dropping the draft (whose overlay the kernel has been seeing) does
    /// not flip them to whatever the listing says: a listing that still
    /// lags the upload, or a server clock older than ours, would otherwise
    /// make size/mtime jump backwards — the attribute instability Apple's
    /// NFS client can panic on (fuse-t #109). `etag`, when non-empty, is
    /// the new version's; `ensure_listed` then keeps these size/mtime for
    /// as long as the listing reports that same etag. The mtime never moves
    /// backwards from what the tree already had.
    pub async fn record_upload(&self, remote_path: &str, size: u64, mtime_secs: i64, etag: &str) {
        let mut inner = self.inner.write().await;
        let Some(id) = inner
            .attrs
            .iter()
            .find(|(_, attr)| !attr.is_dir && attr.remote_path == remote_path)
            .map(|(id, _)| *id)
        else {
            return;
        };
        if let Some(attr) = inner.attrs.get_mut(&id) {
            attr.size = size;
            attr.mtime_secs = attr.mtime_secs.max(mtime_secs);
            if !etag.is_empty() {
                attr.etag = etag.to_string();
            }
        }
    }

    /// D5 (write-back conflict check): the etag-only projection of
    /// [`refresh_attrs`] — see its doc for the full behavior.
    pub async fn refresh_etag(&self, remote_path: &str) -> Result<Option<String>> {
        Ok(self.refresh_attrs(remote_path).await?.map(|attr| attr.etag))
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

/// Moves a directory's reported mtime forward because its listing changed
/// on the server: to now, or one nanosecond past what the kernel was last
/// told if that is later, so it never repeats or goes backwards.
///
/// This is what makes a remote change visible through NFS at all. Apple's
/// client keeps a directory's cached names, negative entries and READDIR
/// buffers until a GETATTR shows that directory's mtime changed
/// (`kext/nfs_vnops.c`, `nfs_getattr`: `NFS_CHANGED_NC` -> `cache_purge`,
/// `NFS_CHANGED` -> `nfs_invaldir`, both mtime comparisons for NFSv3).
/// Cloudreve's `updated_at` for a folder is not relied on to move when its
/// children change (not verified against the server either way), so the
/// server's value alone could leave the kernel's view stale for as long as
/// it stays cached. It is also `nfs3_server`'s READDIR
/// cookie verifier, which is what a changed directory should get.
fn bump_dir_mtime(attr: &mut NodeAttr) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let now = (now.as_secs() as i64, now.subsec_nanos());
    let next = if attr.mtime_nsecs >= 999_999_999 {
        (attr.mtime_secs + 1, 0)
    } else {
        (attr.mtime_secs, attr.mtime_nsecs + 1)
    };
    (attr.mtime_secs, attr.mtime_nsecs) = now.max(next);
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
    inner.stale_files.remove(&id);
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
