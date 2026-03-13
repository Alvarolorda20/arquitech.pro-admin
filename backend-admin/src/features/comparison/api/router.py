"""Comparison feature API routes."""

from fastapi import APIRouter

from src.features.comparison.application.use_cases.http_handlers import (
    download_input_file,
    download_result,
    get_credit_balance,
    get_credit_estimate,
    get_job_status,
    health_check,
    process_budget,
    rerun_budget_from_last_inputs,
    rerun_budget_with_pdf_overrides,
)

router = APIRouter(tags=["comparison"])

router.add_api_route("/api/process-budget", process_budget, methods=["POST"])
router.add_api_route("/api/process-budget/rerun", rerun_budget_from_last_inputs, methods=["POST"])
router.add_api_route("/api/process-budget/rerun-with-overrides", rerun_budget_with_pdf_overrides, methods=["POST"])
router.add_api_route("/api/credits/balance", get_credit_balance, methods=["GET"])
router.add_api_route("/api/credits/estimate", get_credit_estimate, methods=["GET"])
router.add_api_route("/api/status/{job_id}", get_job_status, methods=["GET"])
router.add_api_route("/api/download/{job_id}", download_result, methods=["GET"])
router.add_api_route("/api/download-input/{job_id}", download_input_file, methods=["GET"])
router.add_api_route("/health", health_check, methods=["GET"])
