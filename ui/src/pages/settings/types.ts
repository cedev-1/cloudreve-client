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
}

export type DriveStatus = "active" | "event_push_lost" | "offline" | "credential_expired";

export interface CapacitySummary {
  total: number;
  used: number;
  label: string;
}
