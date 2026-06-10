-- v0.9 (M4): soft-delete profiles. Additive; existing rows have deleted_at NULL.
ALTER TABLE profiles ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_profiles_deleted_at ON profiles(deleted_at);
