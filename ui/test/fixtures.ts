// Payloads shaped like the Rust serde output the webviews receive:
// StatusSummary / DriveSummary / TaskWithProgress (#[serde(flatten)] task) /
// ConflictInfo / DriveInfo in crates/cloudreve-sync/src/drive/manager/types.rs,
// TaskRecord in crates/cloudreve-sync/src/inventory/models.rs, TaskProgress in
// crates/cloudreve-sync/src/tasks/types.rs, GeneralSettings and UserProfile in
// src-tauri/src/commands.rs. `Option::None` serializes as null unless the
// field has skip_serializing_if (User).

const now = () => Math.floor(Date.now() / 1000);

export const photosDrive = {
  id: "drive-photos",
  name: "Photos",
  instance_url: "https://cloud.example.com",
  remote_path: "cloudreve://my/Photos",
  sync_path: "/Users/alice/Photos",
  icon_path: null,
  raw_icon_path: null,
  user_id: "u1",
  mode: "full_mirror",
};

export const workDrive = {
  id: "drive-work",
  name: "Work",
  instance_url: "https://cloud.example.com",
  remote_path: "cloudreve://my/Work",
  sync_path: "/Users/alice/Work",
  icon_path: "/Users/alice/.cloudreve/icos/work.ico",
  raw_icon_path: "/Users/alice/.cloudreve/icos/work.png",
  user_id: "u1",
  mode: "on_demand",
};

function taskRecord(over: Record<string, unknown>) {
  return {
    id: "t",
    drive_id: photosDrive.id,
    task_type: "download",
    local_path: "/Users/alice/Photos/file.bin",
    status: "Completed",
    progress: 1,
    total_bytes: 0,
    processed_bytes: 0,
    priority: 0,
    custom_state: null,
    error: null,
    created_at: now() - 600,
    updated_at: now() - 600,
    ...over,
  };
}

export function emptySummary(over: Record<string, unknown> = {}) {
  return {
    drives: [photosDrive, workDrive],
    active_tasks: [],
    finished_tasks: [],
    has_ever_synced: true,
    conflicts: [],
    paused_drives: [],
    pending_uploads: { [workDrive.id]: 0 },
    mount_errors: {},
    upload_problems: {},
    ...over,
  };
}

export function busySummary() {
  return emptySummary({
    active_tasks: [
      {
        ...taskRecord({
          id: "up-1",
          task_type: "upload",
          local_path: "/Users/alice/Photos/Holidays/beach.jpg",
          status: "Running",
          progress: 0.5,
          total_bytes: 4 * 1024 * 1024,
          processed_bytes: 2 * 1024 * 1024,
          updated_at: now(),
        }),
        live_progress: {
          task_id: "up-1",
          kind: "Upload",
          local_path: "/Users/alice/Photos/Holidays/beach.jpg",
          progress: 0.5,
          processed_bytes: 2 * 1024 * 1024,
          total_bytes: 4 * 1024 * 1024,
          speed_bytes_per_sec: 512 * 1024,
          eta_seconds: 4,
          custom_state: null,
        },
      },
      {
        ...taskRecord({
          id: "down-1",
          local_path: "/Users/alice/Photos/report.pdf",
          status: "Pending",
          progress: 0,
          updated_at: now(),
        }),
        live_progress: null,
      },
    ],
    finished_tasks: [
      taskRecord({
        id: "done-1",
        local_path: "/Users/alice/Photos/Trips/rome.jpg",
        updated_at: now() - 5 * 60 - 10,
      }),
      taskRecord({
        id: "fail-1",
        task_type: "upload",
        local_path: "/Users/alice/Photos/huge.iso",
        status: "Failed",
        error: "File is larger than the 3072 MB limit",
      }),
    ],
    conflicts: [
      {
        id: 7,
        drive_id: photosDrive.id,
        drive_name: photosDrive.name,
        local_path: "/Users/alice/Photos/Budget/budget.xlsx",
        synced_size: 1200,
        local_size: 1350,
        local_modified_at: now() - 120,
      },
    ],
    pending_uploads: { [workDrive.id]: 3 },
  });
}

export function driveInfo(over: Record<string, unknown>) {
  return {
    id: photosDrive.id,
    name: photosDrive.name,
    instance_url: photosDrive.instance_url,
    remote_path: photosDrive.remote_path,
    sync_path: photosDrive.sync_path,
    icon_path: null,
    raw_icon_path: null,
    enabled: true,
    paused: false,
    user_id: "u1",
    status: "active",
    capacity: { total: 10 * 1024 ** 3, used: 2 * 1024 ** 3, label: "2.0 GB / 10.0 GB (20.0%)" },
    mode: "full_mirror",
    mount_error: null,
    unsynced_files: 0,
    ...over,
  };
}

export const mirrorDriveInfo = driveInfo({});

export const MOUNT_ERROR =
  "The drive folder could not be mounted: the folder is in use by another program.";

export const onDemandDriveInfo = driveInfo({
  id: workDrive.id,
  name: workDrive.name,
  remote_path: workDrive.remote_path,
  sync_path: workDrive.sync_path,
  icon_path: workDrive.icon_path,
  raw_icon_path: workDrive.raw_icon_path,
  status: "mount_failed",
  mode: "on_demand",
  capacity: null,
  mount_error: MOUNT_ERROR,
  unsynced_files: 3,
});

export const expiredDriveInfo = driveInfo({
  id: "drive-old",
  name: "Archive",
  instance_url: "https://old.example.org",
  remote_path: "cloudreve://my/Archive",
  sync_path: "/Users/alice/Archive",
  status: "credential_expired",
  capacity: null,
});

export const generalSettings = {
  notify_credential_expired: true,
  notify_file_conflict: false,
  fast_popup_launch: true,
  log_to_file: true,
  log_level: "info",
  log_max_files: 5,
  log_dir: "/Users/alice/.cloudreve/logs",
  language: null,
  heartbeat_interval: 15,
  check_updates_on_startup: true,
  auto_install_updates: false,
};

export const userProfile = {
  user: {
    id: "u1",
    email: "alice@example.com",
    nickname: "Alice",
    status: "active",
    avatar: "file",
    created_at: "2024-03-15T10:20:30Z",
    group: { id: "g1", name: "Family" },
  },
  avatar_url: "https://cloud.example.com/api/v4/user/avatar/u1",
  profile_url: "https://cloud.example.com/profile/u1",
};
