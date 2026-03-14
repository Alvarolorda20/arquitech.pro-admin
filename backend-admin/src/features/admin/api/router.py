"""Admin feature API routes."""

from fastapi import APIRouter

from src.features.admin.application.use_cases.http_handlers import (
    admin_login,
    admin_login_portal,
    admin_refresh,
    admin_memberships_panel,
    delete_membership,
    download_admin_run_artifact,
    get_admin_tenant_overview,
    get_admin_run_artifacts,
    patch_membership_role,
    patch_membership_status,
    patch_tenant_billing_config,
    patch_tenant_credits_adjust,
    patch_tenant_subscription_status,
)

router = APIRouter(tags=["admin"])


async def _health_check() -> dict:
    return {"status": "ok"}


router.add_api_route("/health", _health_check, methods=["GET"], tags=["health"])
router.add_api_route("/admin", admin_login_portal, methods=["GET"])
router.add_api_route("/api/admin/login", admin_login, methods=["POST"])
router.add_api_route("/api/admin/refresh", admin_refresh, methods=["POST"])
router.add_api_route("/admin/memberships", admin_memberships_panel, methods=["GET"])
router.add_api_route("/api/admin/tenant-overview", get_admin_tenant_overview, methods=["GET"])
router.add_api_route("/api/admin/run-artifacts", get_admin_run_artifacts, methods=["GET"])
router.add_api_route("/api/admin/run-artifact/download", download_admin_run_artifact, methods=["GET"])
router.add_api_route("/api/admin/memberships/status", patch_membership_status, methods=["PATCH"])
router.add_api_route("/api/admin/memberships/role", patch_membership_role, methods=["PATCH"])
router.add_api_route("/api/admin/memberships", delete_membership, methods=["DELETE"])
router.add_api_route(
    "/api/admin/tenant-subscriptions/status",
    patch_tenant_subscription_status,
    methods=["PATCH"],
)
router.add_api_route(
    "/api/admin/tenant-credits/adjust",
    patch_tenant_credits_adjust,
    methods=["PATCH"],
)
router.add_api_route(
    "/api/admin/tenant-billing-config",
    patch_tenant_billing_config,
    methods=["PATCH"],
)
