from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests


class SupabaseRunRepository:
    """Small REST repository for persisting budget execution metadata."""

    def __init__(self, base_url: str, service_role_key: str, timeout: int = 20):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.storage_bucket = (
            os.getenv("SUPABASE_STORAGE_BUCKET", "").strip() or "budget-artifacts"
        )
        self._headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json",
        }
        self._budget_runs_available: bool | None = None
        self._storage_bucket_verified: bool = False

    @classmethod
    def from_env(cls) -> "SupabaseRunRepository | None":
        base = os.getenv("SUPABASE_URL", "").strip()
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        if not base or not key:
            return None
        return cls(base, key)

    def is_enabled(self) -> bool:
        return True

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: Any | None = None,
        prefer: str | None = None,
        allow_not_found: bool = False,
    ) -> tuple[int, Any]:
        headers = dict(self._headers)
        if prefer:
            headers["Prefer"] = prefer

        url = f"{self.base_url}{path}"
        resp = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            data=json.dumps(payload) if payload is not None else None,
            timeout=self.timeout,
        )

        if allow_not_found and resp.status_code == 404:
            return resp.status_code, None

        if not (200 <= resp.status_code < 300):
            body = resp.text.strip()
            if len(body) > 800:
                body = body[:800] + "..."
            raise RuntimeError(f"Supabase REST {resp.status_code} {method} {path}: {body}")

        if not resp.text:
            return resp.status_code, None

        try:
            return resp.status_code, resp.json()
        except Exception:
            return resp.status_code, resp.text

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _storage_headers(self, *, content_type: str) -> dict[str, str]:
        return {
            "apikey": self._headers["apikey"],
            "Authorization": self._headers["Authorization"],
            "Content-Type": content_type,
        }

    @staticmethod
    def _safe_json(resp: requests.Response) -> dict[str, Any]:
        try:
            payload = resp.json()
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _truncated_body(resp: requests.Response) -> str:
        body = resp.text.strip()
        if len(body) > 800:
            return body[:800] + "..."
        return body

    @classmethod
    def _is_bucket_not_found(cls, resp: requests.Response) -> bool:
        if resp.status_code == 404:
            return True
        payload = cls._safe_json(resp)
        if str(payload.get("statusCode") or "").strip() == "404":
            return True
        return "bucket not found" in (resp.text or "").lower()

    @classmethod
    def _is_bucket_already_exists(cls, resp: requests.Response) -> bool:
        if resp.status_code == 409:
            return True
        payload = cls._safe_json(resp)
        status_code = str(payload.get("statusCode") or "").strip()
        if status_code == "409":
            return True
        body = (resp.text or "").lower()
        return "already exists" in body and "bucket" in body

    def _ensure_storage_bucket(self) -> str:
        if self._storage_bucket_verified:
            return self.storage_bucket

        bucket = self.storage_bucket
        url = f"{self.base_url}/storage/v1/bucket/{quote(bucket, safe='')}"
        resp = requests.get(url, headers=self._storage_headers(content_type="application/json"), timeout=self.timeout)
        if self._is_bucket_not_found(resp):
            create_url = f"{self.base_url}/storage/v1/bucket"
            payload = {"id": bucket, "name": bucket, "public": False}
            created = requests.post(
                create_url,
                headers=self._storage_headers(content_type="application/json"),
                data=json.dumps(payload),
                timeout=self.timeout,
            )
            if not (200 <= created.status_code < 300) and not self._is_bucket_already_exists(created):
                body = self._truncated_body(created)
                raise RuntimeError(
                    f"Supabase Storage {created.status_code} POST /bucket: {body}"
                )
        elif not (200 <= resp.status_code < 300):
            body = self._truncated_body(resp)
            raise RuntimeError(
                f"Supabase Storage {resp.status_code} GET /bucket/{bucket}: {body}"
            )

        self._storage_bucket_verified = True
        return bucket

    def upload_bytes(
        self,
        *,
        object_path: str,
        data: bytes,
        content_type: str,
        upsert: bool = True,
    ) -> dict[str, str]:
        bucket = self._ensure_storage_bucket()
        clean_path = object_path.strip("/")
        encoded_path = quote(clean_path, safe="/._-")
        url = f"{self.base_url}/storage/v1/object/{quote(bucket, safe='')}/{encoded_path}"
        headers = self._storage_headers(content_type=content_type)
        if upsert:
            headers["x-upsert"] = "true"

        resp = requests.post(
            url,
            headers=headers,
            data=data,
            timeout=self.timeout,
        )
        if not (200 <= resp.status_code < 300):
            body = resp.text.strip()
            if len(body) > 800:
                body = body[:800] + "..."
            raise RuntimeError(
                f"Supabase Storage {resp.status_code} POST /object/{bucket}/{clean_path}: {body}"
            )

        return {"bucket": bucket, "path": clean_path}

    def download_bytes(
        self,
        *,
        bucket: str,
        object_path: str,
    ) -> bytes:
        clean_bucket = (bucket or "").strip()
        clean_path = (object_path or "").strip().strip("/")
        if not clean_bucket:
            raise ValueError("bucket is required to download from Supabase Storage.")
        if not clean_path:
            raise ValueError("object_path is required to download from Supabase Storage.")

        encoded_bucket = quote(clean_bucket, safe="")
        encoded_path = quote(clean_path, safe="/._-")
        url = f"{self.base_url}/storage/v1/object/{encoded_bucket}/{encoded_path}"
        resp = requests.get(
            url,
            headers={
                "apikey": self._headers["apikey"],
                "Authorization": self._headers["Authorization"],
            },
            timeout=self.timeout,
        )
        if not (200 <= resp.status_code < 300):
            body = self._truncated_body(resp)
            raise RuntimeError(
                f"Supabase Storage {resp.status_code} GET /object/{clean_bucket}/{clean_path}: {body}"
            )
        return resp.content

    def get_project_context(self, project_id: str) -> dict[str, Any] | None:
        _, data = self._request(
            "GET",
            "/rest/v1/projects",
            params={
                "select": "id,tenant_id,created_by,name,status",
                "id": f"eq.{project_id}",
                "limit": "1",
            },
        )
        if not data:
            return None
        return data[0]

    def create_task_run(
        self,
        *,
        tenant_id: str,
        project_id: str,
        created_by: str,
        title: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        now = self._now_iso()
        row = {
            "id": str(uuid.uuid4()),
            "tenant_id": tenant_id,
            "project_id": project_id,
            "title": title,
            "description": "Pipeline execution run",
            "status": "in_progress",
            "payload": payload,
            "created_by": created_by,
            "created_at": now,
            "updated_at": now,
        }
        _, data = self._request(
            "POST",
            "/rest/v1/tasks",
            payload=row,
            prefer="return=representation",
        )
        return data[0] if data else row

    def update_task_run(self, task_id: str, *, status: str, payload: dict[str, Any]) -> None:
        self._request(
            "PATCH",
            "/rest/v1/tasks",
            params={"id": f"eq.{task_id}"},
            payload={
                "status": status,
                "payload": payload,
                "updated_at": self._now_iso(),
            },
            prefer="return=minimal",
        )

    def create_document(
        self,
        *,
        tenant_id: str,
        project_id: str,
        created_by: str,
        title: str,
        document_type: str,
        content: dict[str, Any],
        status: str = "draft",
        source_hash: str | None = None,
        source_size_bytes: int | None = None,
        source_mime: str | None = None,
    ) -> dict[str, Any]:
        now = self._now_iso()
        row = {
            "id": str(uuid.uuid4()),
            "tenant_id": tenant_id,
            "project_id": project_id,
            "title": title,
            "document_type": document_type,
            "status": status,
            "content": content,
            "source_hash": source_hash,
            "source_size_bytes": source_size_bytes,
            "source_mime": source_mime,
            "created_by": created_by,
            "created_at": now,
            "updated_at": now,
        }
        _, data = self._request(
            "POST",
            "/rest/v1/documents",
            payload=row,
            prefer="return=representation",
        )
        return data[0] if data else row

    def create_extraction(
        self,
        *,
        tenant_id: str,
        project_id: str,
        created_by: str,
        document_id: str | None,
        run_id: str | None,
        extraction_signature: str | None,
        provider: str,
        status: str,
        raw_payload: dict[str, Any],
        normalized_payload: Any,
        warnings: list[Any] | None = None,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        now = self._now_iso()
        row = {
            "id": str(uuid.uuid4()),
            "tenant_id": tenant_id,
            "project_id": project_id,
            "document_id": document_id,
            "run_id": run_id,
            "extraction_signature": extraction_signature,
            "provider": provider,
            "status": status,
            "raw_payload": raw_payload,
            "normalized_payload": normalized_payload,
            "field_confidence": {},
            "warnings": warnings or [],
            "error_message": error_message,
            "created_by": created_by,
            "created_at": now,
            "updated_at": now,
        }
        _, data = self._request(
            "POST",
            "/rest/v1/extractions",
            payload=row,
            prefer="return=representation",
        )
        return data[0] if data else row

    def upsert_variable(
        self,
        *,
        tenant_id: str,
        project_id: str,
        created_by: str,
        variable_key: str,
        value: Any,
        source: str = "budget-backend",
        confidence: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        _, existing = self._request(
            "GET",
            "/rest/v1/variables",
            params={
                "select": "id",
                "tenant_id": f"eq.{tenant_id}",
                "project_id": f"eq.{project_id}",
                "variable_key": f"eq.{variable_key}",
                "limit": "1",
            },
        )

        update_payload = {
            "value": value,
            "source": source,
            "confidence": confidence,
            "metadata": metadata or {},
            "updated_at": self._now_iso(),
        }

        if existing:
            var_id = existing[0]["id"]
            self._request(
                "PATCH",
                "/rest/v1/variables",
                params={"id": f"eq.{var_id}"},
                payload=update_payload,
                prefer="return=minimal",
            )
            return

        now = self._now_iso()
        row = {
            "id": str(uuid.uuid4()),
            "tenant_id": tenant_id,
            "project_id": project_id,
            "variable_key": variable_key,
            "value": value,
            "source": source,
            "confidence": confidence,
            "metadata": metadata or {},
            "created_by": created_by,
            "created_at": now,
            "updated_at": now,
        }
        self._request(
            "POST",
            "/rest/v1/variables",
            payload=row,
            prefer="return=minimal",
        )

    def _has_budget_runs_table(self) -> bool:
        if self._budget_runs_available is not None:
            return self._budget_runs_available

        status, _ = self._request(
            "GET",
            "/rest/v1/budget_runs",
            params={"select": "id", "limit": "1"},
            allow_not_found=True,
        )
        self._budget_runs_available = status != 404
        return self._budget_runs_available

    def create_budget_run(
        self,
        *,
        tenant_id: str,
        project_id: str,
        created_by: str,
        task_id: str | None,
        pipeline_job_id: str,
        force_rerun: bool,
        request_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not self._has_budget_runs_table():
            return None

        now = self._now_iso()
        row = {
            "id": str(uuid.uuid4()),
            "tenant_id": tenant_id,
            "project_id": project_id,
            "task_id": task_id,
            "pipeline_job_id": pipeline_job_id,
            "status": "running",
            "force_rerun": force_rerun,
            "request_payload": request_payload,
            "result_payload": {},
            "created_by": created_by,
            "started_at": now,
            "created_at": now,
            "updated_at": now,
        }
        _, data = self._request(
            "POST",
            "/rest/v1/budget_runs",
            payload=row,
            prefer="return=representation",
        )
        return data[0] if data else row

    def update_budget_run(
        self,
        run_id: str,
        *,
        status: str,
        result_payload: dict[str, Any],
        error_message: str | None,
    ) -> None:
        if not self._has_budget_runs_table():
            return

        update = {
            "status": status,
            "result_payload": result_payload,
            "error_message": error_message,
            "finished_at": self._now_iso(),
            "updated_at": self._now_iso(),
        }
        self._request(
            "PATCH",
            "/rest/v1/budget_runs",
            params={"id": f"eq.{run_id}"},
            payload=update,
            prefer="return=minimal",
        )

    def update_budget_run_request_payload(self, run_id: str, request_payload: dict[str, Any]) -> None:
        if not self._has_budget_runs_table():
            return
        self._request(
            "PATCH",
            "/rest/v1/budget_runs",
            params={"id": f"eq.{run_id}"},
            payload={
                "request_payload": request_payload,
                "updated_at": self._now_iso(),
            },
            prefer="return=minimal",
        )

    def get_budget_run_by_id(self, run_id: str) -> dict[str, Any] | None:
        if not self._has_budget_runs_table():
            return None
        _, data = self._request(
            "GET",
            "/rest/v1/budget_runs",
            params={
                "select": "id,tenant_id,project_id,pipeline_job_id,status,started_at,finished_at,request_payload,result_payload,error_message,created_at,updated_at",
                "id": f"eq.{run_id}",
                "limit": "1",
            },
            allow_not_found=True,
        )
        if not data:
            return None
        return data[0]

    def list_extractions_by_run_id(self, run_id: str) -> list[dict[str, Any]]:
        _, data = self._request(
            "GET",
            "/rest/v1/extractions",
            params={
                "select": "id,run_id,status,raw_payload,created_at",
                "run_id": f"eq.{run_id}",
                "order": "created_at.asc",
            },
            allow_not_found=True,
        )
        if not data:
            return []
        return data

    def get_budget_run_by_pipeline_job_id(self, pipeline_job_id: str) -> dict[str, Any] | None:
        if not self._has_budget_runs_table():
            return None
        _, data = self._request(
            "GET",
            "/rest/v1/budget_runs",
            params={
                "select": "id,tenant_id,project_id,pipeline_job_id,status,started_at,finished_at,request_payload,result_payload,error_message,created_at,updated_at",
                "pipeline_job_id": f"eq.{pipeline_job_id}",
                "order": "started_at.desc",
                "limit": "1",
            },
            allow_not_found=True,
        )
        if not data:
            return None
        return data[0]
