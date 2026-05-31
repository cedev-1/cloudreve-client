import {
  Avatar,
  Box,
  Button,
  CircularProgress,
  Typography,
} from "@mui/material";
import { openUrl } from "@tauri-apps/plugin-opener";
import { invoke } from "@tauri-apps/api/core";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

interface UserProfile {
  user: {
    id: string;
    email?: string;
    nickname: string;
    status?: "active" | "inactive" | "manual_banned" | "sys_banned";
    avatar?: "gravatar" | "file" | "";
    created_at: string;
    group?: { id: string; name: string };
  };
  avatar_url: string;
  profile_url: string;
}

const STATUS_COLORS: Record<string, "success" | "warning" | "error" | "default"> = {
  active: "success",
  inactive: "warning",
  manual_banned: "error",
  sys_banned: "error",
};

export default function ProfileSection() {
  const { t } = useTranslation();
  const [profile, setProfile] = useState<UserProfile | null>(null);
  const [loading, setLoading] = useState(true);
  const [avatarError, setAvatarError] = useState(false);

  useEffect(() => {
    const loadProfile = async () => {
      try {
        const result = await invoke<UserProfile | null>("get_user_profile");
        setProfile(result);
      } catch (error) {
        console.error("Failed to load profile:", error);
      } finally {
        setLoading(false);
      }
    };
    loadProfile();
  }, []);

  const handleOpenProfile = async () => {
    if (profile) {
      await openUrl(profile.profile_url);
    }
  };

  if (loading) {
    return (
      <Box sx={{ display: "flex", justifyContent: "center", py: 8 }}>
        <CircularProgress size={32} />
      </Box>
    );
  }

  if (!profile) {
    return (
      <Box sx={{ py: 4, textAlign: "center" }}>
        <Typography variant="body2" color="text.secondary">
          {t("settings.profileNoDrive")}
        </Typography>
      </Box>
    );
  }

  const { user, avatar_url } = profile;
  const status = user.status ?? "active";
  const memberSince = new Date(user.created_at).toLocaleDateString(undefined, {
    year: "numeric",
    month: "long",
    day: "numeric",
  });

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        py: 3,
        gap: 2,
      }}
    >
      <Avatar
        src={avatarError ? undefined : avatar_url}
        alt={user.nickname}
        sx={{ width: 80, height: 80 }}
        onError={() => setAvatarError(true)}
      />

      <Box sx={{ textAlign: "center" }}>
        <Typography variant="h6" fontWeight={600}>
          {user.nickname}
        </Typography>
        {user.email && (
          <Typography variant="body2" color="text.secondary">
            {user.email}
          </Typography>
        )}
      </Box>

      <Box sx={{ display: "flex", alignItems: "center", gap: 0.8 }}>
        <Box
          sx={{
            width: 8,
            height: 8,
            borderRadius: "50%",
            bgcolor: `${STATUS_COLORS[status] ?? "grey"}.main`,
          }}
        />
        <Typography variant="body2" color="text.secondary">
          {t(`settings.profileStatus.${status}`, status)}
        </Typography>
      </Box>

      <Box
        sx={{
          width: "100%",
          mt: 2,
          border: 1,
          borderColor: "divider",
          borderRadius: 1,
          overflow: "hidden",
          bgcolor: (theme) =>
            theme.palette.mode === "light"
              ? theme.palette.grey[50]
              : theme.palette.grey[900],
        }}
      >
        {user.group && (
          <ProfileRow
            label={t("settings.profileGroup")}
            value={user.group.name}
          />
        )}
        <ProfileRow
          label={t("settings.profileSince")}
          value={memberSince}
          isLast
        />
      </Box>

      <Button variant="outlined" size="small" onClick={handleOpenProfile}>
        {t("settings.openSite")}
      </Button>
    </Box>
  );
}

function ProfileRow({
  label,
  value,
  isLast,
}: {
  label: string;
  value: string;
  isLast?: boolean;
}) {
  return (
    <Box
      sx={{
        display: "flex",
        justifyContent: "space-between",
        px: 2,
        py: 1.5,
        borderBottom: isLast ? "none" : 1,
        borderColor: "divider",
      }}
    >
      <Typography variant="body2" color="text.secondary">
        {label}
      </Typography>
      <Typography variant="body2">{value}</Typography>
    </Box>
  );
}
