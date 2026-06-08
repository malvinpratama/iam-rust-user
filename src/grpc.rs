//! Tonic implementation of UserService.

use tonic::{Request, Response, Status};
use uuid::Uuid;

use proto::user::v1::user_service_server::UserService;
use proto::user::v1::*;

use crate::repo::{ProfileRow, Repo};

pub struct UserSvc {
    repo: Repo,
}

impl UserSvc {
    pub fn new(repo: Repo) -> Self {
        Self { repo }
    }
}

#[tonic::async_trait]
impl UserService for UserSvc {
    async fn create_profile(
        &self,
        request: Request<CreateProfileRequest>,
    ) -> Result<Response<Profile>, Status> {
        let req = request.into_inner();
        let user_id = parse_id(&req.user_id)?;
        let row = self
            .repo
            .create_profile(user_id, &req.display_name)
            .await
            .map_err(|_| Status::already_exists("profile already exists"))?;
        Ok(Response::new(to_proto(row)))
    }

    async fn get_profile(
        &self,
        request: Request<GetProfileRequest>,
    ) -> Result<Response<Profile>, Status> {
        let user_id = parse_id(&request.into_inner().user_id)?;
        let row = self
            .repo
            .get_profile(user_id)
            .await
            .map_err(|_| Status::internal("db error"))?
            .ok_or_else(|| Status::not_found("profile not found"))?;
        Ok(Response::new(to_proto(row)))
    }

    async fn update_profile(
        &self,
        request: Request<UpdateProfileRequest>,
    ) -> Result<Response<Profile>, Status> {
        let req = request.into_inner();
        let user_id = parse_id(&req.user_id)?;
        let row = self
            .repo
            .update_profile(user_id, req.display_name, req.bio, req.avatar_url, req.phone)
            .await
            .map_err(|_| Status::internal("db error"))?
            .ok_or_else(|| Status::not_found("profile not found"))?;
        Ok(Response::new(to_proto(row)))
    }

    async fn delete_profile(
        &self,
        request: Request<DeleteProfileRequest>,
    ) -> Result<Response<DeleteProfileResponse>, Status> {
        let user_id = parse_id(&request.into_inner().user_id)?;
        self.repo
            .delete_profile(user_id)
            .await
            .map_err(|_| Status::internal("failed to delete profile"))?;
        Ok(Response::new(DeleteProfileResponse { success: true }))
    }

    async fn list_profiles(
        &self,
        request: Request<ListProfilesRequest>,
    ) -> Result<Response<ListProfilesResponse>, Status> {
        let req = request.into_inner();
        let page = if req.page < 1 { 1 } else { req.page };
        let size = match req.page_size {
            n if n < 1 => 20,
            n if n > 100 => 100,
            n => n,
        };
        let offset = (page - 1) * size;

        let rows = self
            .repo
            .list_profiles(&req.query, size as i64, offset as i64)
            .await
            .map_err(|_| Status::internal("failed to list profiles"))?;
        let total = self
            .repo
            .count_profiles(&req.query)
            .await
            .map_err(|_| Status::internal("failed to count profiles"))?;
        Ok(Response::new(ListProfilesResponse {
            profiles: rows.into_iter().map(to_proto).collect(),
            total: total as i32,
            page,
            page_size: size,
        }))
    }
}

fn parse_id(s: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(s).map_err(|_| Status::invalid_argument("invalid user id"))
}

fn to_proto(p: ProfileRow) -> Profile {
    Profile {
        user_id: p.user_id.to_string(),
        display_name: p.display_name,
        bio: p.bio,
        avatar_url: p.avatar_url,
        phone: p.phone,
        created_at: p.created_at.to_rfc3339(),
        updated_at: p.updated_at.to_rfc3339(),
    }
}
