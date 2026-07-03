import {
  Box,
  Button,
  CircularProgress,
  Link,
  ListItem,
  ListItemIcon,
  ListItemText,
  Menu,
  MenuItem,
  Typography,
} from "@mui/material";
import {
  Warning as WarningIcon,
  ExpandMore as ExpandMoreIcon,
  Laptop as LaptopIcon,
  Cloud as CloudIcon,
  CallSplit as CallSplitIcon,
} from "@mui/icons-material";
import { invoke } from "@tauri-apps/api/core";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { ConflictInfo, ConflictResolution } from "./types";
import { getFileName, getParentFolderName } from "./utils";
import FileIcon from "./FileIcon";

interface ConflictItemProps {
  conflict: ConflictInfo;
  onResolved: () => void;
}

export default function ConflictItem({ conflict, onResolved }: ConflictItemProps) {
  const { t } = useTranslation();
  const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);
  const [resolving, setResolving] = useState(false);

  const fileName = getFileName(conflict.local_path);
  const parentFolderName = getParentFolderName(conflict.local_path);

  const handleShowInExplorer = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    invoke("show_file_in_explorer", { path: conflict.local_path });
  };

  const handleResolve = async (resolution: ConflictResolution) => {
    setAnchorEl(null);
    setResolving(true);
    try {
      await invoke("resolve_conflict", {
        driveId: conflict.drive_id,
        localPath: conflict.local_path,
        resolution,
      });
      onResolved();
    } catch (error) {
      console.error("Failed to resolve conflict:", error);
      setResolving(false);
    }
  };

  return (
    <ListItem
      sx={{
        px: 2,
        py: 1,
        "&:hover": { bgcolor: "action.hover" },
      }}
      secondaryAction={
        resolving ? (
          <CircularProgress size={18} />
        ) : (
          <Button
            size="small"
            variant="outlined"
            color="warning"
            endIcon={<ExpandMoreIcon />}
            onClick={(e) => setAnchorEl(e.currentTarget)}
            sx={{ textTransform: "none", minWidth: 0, px: 1 }}
          >
            {t("popup.conflictResolve", "Resolve")}
          </Button>
        )
      }
    >
      <ListItemIcon sx={{ minWidth: 40 }}>
        <Box sx={{ position: "relative", width: 28, height: 28 }}>
          <FileIcon path={conflict.local_path} size={28} />
          <Box
            sx={{
              position: "absolute",
              bottom: -4,
              right: -4,
              bgcolor: "background.paper",
              borderRadius: "50%",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              width: 18,
              height: 18,
            }}
          >
            <WarningIcon sx={{ fontSize: 14 }} color="warning" />
          </Box>
        </Box>
      </ListItemIcon>
      <ListItemText
        sx={{ pr: 9 }}
        primary={
          <Typography variant="body2" noWrap sx={{ fontWeight: 500 }}>
            {fileName}
          </Typography>
        }
        secondary={
          <Box>
            <Typography variant="caption" color="warning.main" component="span">
              {t("popup.conflictModifiedBoth", "Modified on both sides")}
            </Typography>
            <Typography variant="caption" color="text.secondary" component="span">
              {" · "}
            </Typography>
            <Link
              component="button"
              variant="caption"
              color="text.secondary"
              onClick={handleShowInExplorer}
              underline="always"
            >
              {parentFolderName}
            </Link>
          </Box>
        }
      />
      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={() => setAnchorEl(null)}
      >
        <MenuItem onClick={() => handleResolve("keep_local")}>
          <ListItemIcon>
            <LaptopIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText
            primary={t("popup.conflictKeepLocal", "Keep local version")}
            secondary={t("popup.conflictKeepLocalHint", "Overwrites the server file")}
            secondaryTypographyProps={{ variant: "caption" }}
          />
        </MenuItem>
        <MenuItem onClick={() => handleResolve("keep_remote")}>
          <ListItemIcon>
            <CloudIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText
            primary={t("popup.conflictKeepRemote", "Keep server version")}
            secondary={t("popup.conflictKeepRemoteHint", "Overwrites the local file")}
            secondaryTypographyProps={{ variant: "caption" }}
          />
        </MenuItem>
        <MenuItem onClick={() => handleResolve("keep_both")}>
          <ListItemIcon>
            <CallSplitIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText
            primary={t("popup.conflictKeepBoth", "Keep both")}
            secondary={t("popup.conflictKeepBothHint", "Renames the local copy")}
            secondaryTypographyProps={{ variant: "caption" }}
          />
        </MenuItem>
      </Menu>
    </ListItem>
  );
}
