export interface DriveInfo {
  id: string;
  name: string;
  instance_url: string;
  sync_path: string;
  icon_path?: string;
  raw_icon_path?: string;
  enabled: boolean;
  user_id: string;
  remote_path: string;
  status: DriveStatus;
  max_file_size_mb?: number;
  capacity?: CapacitySummary;
  mode: DriveMode;
  // On-demand only: why the folder is not mounted, already a localized,
  // plain-language explanation (`DriveInfo::mount_error`).
  mount_error?: string | null;
  // On-demand only: files with changes not on the server yet.
  unsynced_files?: number;
}

export type DriveStatus = "active" | "event_push_lost" | "offline" | "credential_expired" | "mount_failed";

// Must match `DriveMode`'s serde snake_case wire format exactly
// (crates/cloudreve-sync/src/drive/mounts.rs).
export type DriveMode = "full_mirror" | "on_demand";

export interface CapacitySummary {
  total: number;
  used: number;
  label: string;
}
