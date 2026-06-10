//! Postgres access for the user service via sqlx (runtime-checked queries).

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(FromRow)]
pub struct ProfileRow {
    pub user_id: Uuid,
    pub display_name: String,
    pub bio: String,
    pub avatar_url: String,
    pub phone: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct Repo {
    pool: PgPool,
}

impl Repo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_profile(&self, user_id: Uuid, display_name: &str) -> sqlx::Result<ProfileRow> {
        sqlx::query_as::<_, ProfileRow>(
            "INSERT INTO profiles (user_id, display_name) VALUES ($1, $2) \
             RETURNING user_id, display_name, bio, avatar_url, phone, created_at, updated_at",
        )
        .bind(user_id)
        .bind(display_name)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn get_profile(&self, user_id: Uuid) -> sqlx::Result<Option<ProfileRow>> {
        sqlx::query_as::<_, ProfileRow>(
            "SELECT user_id, display_name, bio, avatar_url, phone, created_at, updated_at \
             FROM profiles WHERE user_id = $1 AND deleted_at IS NULL",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn update_profile(
        &self,
        user_id: Uuid,
        display_name: Option<String>,
        bio: Option<String>,
        avatar_url: Option<String>,
        phone: Option<String>,
    ) -> sqlx::Result<Option<ProfileRow>> {
        sqlx::query_as::<_, ProfileRow>(
            "UPDATE profiles SET \
               display_name = COALESCE($2, display_name), \
               bio          = COALESCE($3, bio), \
               avatar_url   = COALESCE($4, avatar_url), \
               phone        = COALESCE($5, phone), \
               updated_at   = now() \
             WHERE user_id = $1 AND deleted_at IS NULL \
             RETURNING user_id, display_name, bio, avatar_url, phone, created_at, updated_at",
        )
        .bind(user_id)
        .bind(display_name)
        .bind(bio)
        .bind(avatar_url)
        .bind(phone)
        .fetch_optional(&self.pool)
        .await
    }

    /// Soft-delete (stamps deleted_at); the row is kept for restore.
    pub async fn delete_profile(&self, user_id: Uuid) -> sqlx::Result<()> {
        sqlx::query("UPDATE profiles SET deleted_at = now(), updated_at = now() WHERE user_id = $1 AND deleted_at IS NULL")
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn restore_profile(&self, user_id: Uuid) -> sqlx::Result<()> {
        sqlx::query("UPDATE profiles SET deleted_at = NULL, updated_at = now() WHERE user_id = $1")
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn hard_delete_profile(&self, user_id: Uuid) -> sqlx::Result<()> {
        sqlx::query("DELETE FROM profiles WHERE user_id = $1")
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Idempotent profile creation for the event consumer (at-least-once delivery).
    /// On re-registration of a previously soft-deleted user, clear deleted_at.
    pub async fn upsert_profile(&self, user_id: Uuid, display_name: &str) -> sqlx::Result<()> {
        sqlx::query(
            "INSERT INTO profiles (user_id, display_name) VALUES ($1, $2) \
             ON CONFLICT (user_id) DO UPDATE SET deleted_at = NULL",
        )
        .bind(user_id)
        .bind(display_name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_profiles(
        &self,
        query: &str,
        limit: i64,
        offset: i64,
    ) -> sqlx::Result<Vec<ProfileRow>> {
        sqlx::query_as::<_, ProfileRow>(
            "SELECT user_id, display_name, bio, avatar_url, phone, created_at, updated_at \
             FROM profiles \
             WHERE deleted_at IS NULL AND ($1 = '' OR display_name ILIKE '%' || $1 || '%') \
             ORDER BY created_at DESC LIMIT $2 OFFSET $3",
        )
        .bind(query)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn count_profiles(&self, query: &str) -> sqlx::Result<i64> {
        sqlx::query_scalar(
            "SELECT count(*) FROM profiles \
             WHERE deleted_at IS NULL AND ($1 = '' OR display_name ILIKE '%' || $1 || '%')",
        )
        .bind(query)
        .fetch_one(&self.pool)
        .await
    }
}
