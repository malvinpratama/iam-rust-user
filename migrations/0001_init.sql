-- User service schema: profiles keyed by the canonical user_id from AuthService.

CREATE TABLE IF NOT EXISTS profiles (
    user_id      UUID PRIMARY KEY,
    display_name TEXT NOT NULL DEFAULT '',
    bio          TEXT NOT NULL DEFAULT '',
    avatar_url   TEXT NOT NULL DEFAULT '',
    phone        TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_profiles_display_name ON profiles(display_name);
