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
}

export type DriveStatus = "active" | "event_push_lost" | "offline" | "credential_expired";

// Must match `DriveMode`'s serde snake_case wire format exactly
// (crates/cloudreve-sync/src/drive/mounts.rs).
export type DriveMode = "full_mirror" | "on_demand";

export interface CapacitySummary {
  total: number;
  used: number;
  label: string;
}
