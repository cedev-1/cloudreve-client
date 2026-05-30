import { useEffect, useState } from "react";
import {
  Box,
  Typography,
  Link,
  Chip,
  Stack,
  Button,
  LinearProgress,
} from "@mui/material";
import { useTranslation } from "react-i18next";
import { getVersion } from "@tauri-apps/api/app";
import GitHubIcon from "@mui/icons-material/GitHub";
import BugReportIcon from "@mui/icons-material/BugReportRounded";
import ForumIcon from "@mui/icons-material/ForumRounded";
import UpdateIcon from "@mui/icons-material/SystemUpdateAlt";
import logo from "../../assets/cloudreve.svg";
import { HomeRounded } from "@mui/icons-material";
import { check } from "@tauri-apps/plugin-updater";
import { relaunch } from "@tauri-apps/plugin-process";

function isPreviewVersion(version: string): boolean {
  return /alpha|beta|rc|preview|dev/i.test(version);
}

type UpdateState =
  | { status: "idle" }
  | { status: "checking" }
  | { status: "available"; version: string }
  | { status: "downloading"; contentLength: number; downloaded: number }
  | { status: "installing" }
  | { status: "ready" }
  | { status: "up_to_date" }
  | { status: "error"; message: string };

export default function AboutSection() {
  const { t } = useTranslation();
  const [version, setVersion] = useState("");
  const isPreview = version ? isPreviewVersion(version) : false;
  const [updateState, setUpdateState] = useState<UpdateState>({ status: "idle" });

  useEffect(() => {
    getVersion().then(setVersion);
  }, []);

  const links = [
    {
      icon: <HomeRounded fontSize="small" />,
      label: t("about.homepage"),
      href: "https://cloudreve.org",
    },
    {
      icon: <GitHubIcon fontSize="small" />,
      label: "GitHub",
      href: "https://github.com/cedev-1/cloudreve-client",
    },
    {
      icon: <BugReportIcon fontSize="small" />,
      label: t("about.reportIssue"),
      href: "https://github.com/cedev-1/cloudreve-client/issues",
    },
    {
      icon: <ForumIcon fontSize="small" />,
      label: "Discord",
      href: "https://discord.com/invite/WTpMFpZT76",
    },
  ];

  const handleCheckUpdate = async () => {
    setUpdateState({ status: "checking" });
    try {
      const update = await check();
      if (update?.available) {
        setUpdateState({ status: "available", version: update.version });

        let downloaded = 0;
        let contentLength = 0;

        await update.downloadAndInstall((event) => {
          switch (event.event) {
            case "Started":
              contentLength = (event.data as { contentLength?: number })
                .contentLength ?? 0;
              setUpdateState({ status: "downloading", contentLength, downloaded: 0 });
              break;
            case "Progress": {
              const chunkLength = (event.data as { chunkLength?: number })
                .chunkLength ?? 0;
              downloaded += chunkLength;
              setUpdateState({ status: "downloading", contentLength, downloaded });
              break;
            }
            case "Finished":
              setUpdateState({ status: "installing" });
              break;
          }
        });

        setUpdateState({ status: "ready" });
      } else {
        setUpdateState({ status: "up_to_date" });
      }
    } catch (e) {
      setUpdateState({
        status: "error",
        message: e instanceof Error ? e.message : String(e),
      });
    }
  };

  const renderUpdateStatus = () => {
    switch (updateState.status) {
      case "checking":
        return (
          <Typography variant="body2" color="text.secondary">
            {t("about.checking")}
          </Typography>
        );
      case "available":
        return (
          <Typography variant="body2" color="primary">
            {t("about.updateAvailable", { version: updateState.version })}
          </Typography>
        );
      case "downloading": {
        const progress = updateState.contentLength > 0
          ? (updateState.downloaded / updateState.contentLength) * 100
          : 0;
        return (
          <Box sx={{ width: "100%" }}>
            <Typography variant="body2" color="text.secondary">
              {t("about.downloading", { progress: Math.round(progress) })}
            </Typography>
            <LinearProgress variant="determinate" value={progress} sx={{ mt: 0.5 }} />
          </Box>
        );
      }
      case "installing":
        return (
          <Typography variant="body2" color="text.secondary">
            {t("about.installing")}
          </Typography>
        );
      case "ready":
        return (
          <Stack direction="row" spacing={1} alignItems="center">
            <Button size="small" variant="contained" onClick={() => relaunch()}>
              {t("about.restartNow")}
            </Button>
            <Button size="small" onClick={() => setUpdateState({ status: "idle" })}>
              {t("about.restartLater")}
            </Button>
          </Stack>
        );
      case "up_to_date":
        return (
          <Typography variant="body2" color="success.main">
            {t("about.upToDate")}
          </Typography>
        );
      case "error":
        return (
          <Typography variant="body2" color="error">
            {t("about.updateFailed")}
          </Typography>
        );
      default:
        return null;
    }
  };

  return (
    <Box>
      <Stack direction="row" alignItems="center" spacing={2} sx={{ mb: 3 }}>
        <Box
          component="img"
          src={logo}
          alt="Cloudreve"
          sx={{ width: 48, height: 48 }}
        />
        <Box>
          <Typography variant="h6" fontWeight={500}>
            Cloudreve Desktop
          </Typography>
          <Stack direction="row" alignItems="center" spacing={1}>
            <Typography variant="body2" color="text.secondary">
              {version ? `v${version}` : "..."}
            </Typography>
            {isPreview && (
              <Chip
                label={t("about.preview")}
                size="small"
                sx={{ height: 20, fontSize: "0.7rem" }}
              />
            )}
          </Stack>
        </Box>
      </Stack>

      <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 2 }}>
        <Button
          variant="outlined"
          size="small"
          startIcon={<UpdateIcon />}
          onClick={handleCheckUpdate}
          disabled={
            updateState.status === "checking" ||
            updateState.status === "downloading" ||
            updateState.status === "installing"
          }
        >
          {t("about.checkForUpdates")}
        </Button>
      </Stack>

      {updateState.status !== "idle" && (
        <Box sx={{ mb: 2 }}>{renderUpdateStatus()}</Box>
      )}

      <Stack direction="column" spacing={1}>
        {links.map((link) => (
          <Link
            key={link.href}
            href={link.href}
            target="_blank"
            rel="noopener noreferrer"
            underline="hover"
            color="text.secondary"
            sx={{
              display: "inline-flex",
              alignItems: "center",
              gap: 1,
              width: "fit-content",
              "&:hover": { color: "primary.main" },
            }}
          >
            {link.icon}
            <Typography variant="body2">{link.label}</Typography>
          </Link>
        ))}
      </Stack>
    </Box>
  );
}
