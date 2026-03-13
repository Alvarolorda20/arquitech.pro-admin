"""
ADE REST client for LandingAI document extraction.

Handles:
- ``/v1/ade/parse/jobs``          → PDF → async parse job (create + poll)
- ``/v1/ade/parse/jobs/{job_id}`` → poll job status / download result
- ``/v1/ade/extract``             → Markdown + Schema → structured JSON

Parse flow (async Jobs API):
  1. POST /parse/jobs  →  {"job_id": "..."}
  2. Poll GET /parse/jobs/{job_id} every ADE_POLL_INTERVAL seconds.
     Response: {"status": "pending|processing|completed|failed",
                "progress": 0.0-1.0, "data": dict|null, "output_url": str|null}
  3. On "completed":
     - Small docs (< 1 MB): markdown is in response["data"]["markdown"].
     - Large docs (> 1 MB): response["data"] is null; download from output_url.

Configuration via environment variables:
    ADE_BASE_URL        (default: https://api.va.landing.ai/v1/ade)
    ADE_API_KEY         (required — LandingAI API key)
    ADE_TIMEOUT         (default: 1800 s — max total time to wait for job)
    ADE_EXTRACT_TIMEOUT (default: 900 s — used for /extract; LLM processing
                         of large budget PDFs can take 10-15 min)
    ADE_POLL_INTERVAL   (default: 8 s — seconds between status polls)
    ADE_RETRIES         (default: 3)
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class AdeError(Exception):
    """Base exception for all ADE client errors."""


class AdeConfigError(AdeError):
    """Missing or invalid configuration."""


class AdeHTTPError(AdeError):
    """HTTP-level failure (4xx / 5xx / timeout)."""

    def __init__(self, step: str, status: int, body_preview: str):
        self.step = step
        self.status = status
        self.body_preview = body_preview
        super().__init__(f"ADE {step} failed ({status}): {body_preview}")


class AdeParseError(AdeError):
    """Response parsing failed (e.g. missing ``markdown`` or ``chapters``)."""


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

_DEFAULT_BASE = "https://api.va.landing.ai/v1/ade"
_DEFAULT_TIMEOUT = 1800         # max total seconds to wait for a parse job to complete
_DEFAULT_EXTRACT_TIMEOUT = 900  # /extract — LLM processes full Markdown; large docs can take 10-15 min
_DEFAULT_RETRIES = 3
_DEFAULT_POLL_INTERVAL = 8      # seconds between job status polls
_BACKOFF_BASE = 30              # seconds between upload/extract retry attempts
_HTTP_TIMEOUT = 60              # HTTP request timeout for individual poll/upload calls


class AdeClient:
    """
    Stateless REST client wrapping LandingAI ADE endpoints.

    Usage::

        client = AdeClient()                 # reads env vars
        md = client.parse_pdf("offer.pdf")
        data = client.extract(md, schema)
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: int | None = None,
        extract_timeout: int | None = None,
        retries: int | None = None,
        model: str | None = None,
    ):
        self.api_key = api_key or os.getenv("ADE_API_KEY") or os.getenv("VISION_AGENT_API_KEY")
        if not self.api_key:
            raise AdeConfigError(
                "ADE API key is required. "
                "Set ADE_API_KEY or VISION_AGENT_API_KEY in the environment."
            )

        self.base_url = (base_url or os.getenv("ADE_BASE_URL") or _DEFAULT_BASE).rstrip("/")
        # self.timeout = max total seconds to wait for a parse job to finish (polling budget)
        self.timeout = timeout or int(os.getenv("ADE_TIMEOUT", str(_DEFAULT_TIMEOUT)))
        # extract_timeout is intentionally much longer — /extract invokes an LLM
        # over the full Markdown of the document (can be thousands of lines).
        self.extract_timeout = extract_timeout or int(
            os.getenv("ADE_EXTRACT_TIMEOUT", str(_DEFAULT_EXTRACT_TIMEOUT))
        )
        self.retries = retries or int(os.getenv("ADE_RETRIES", str(_DEFAULT_RETRIES)))
        self.poll_interval = int(os.getenv("ADE_POLL_INTERVAL", str(_DEFAULT_POLL_INTERVAL)))
        self.model = model or os.getenv("LANDING_AI_MODEL", "dpt-2").strip() or "dpt-2"

        self._headers = {"Authorization": f"Basic {self.api_key}"}
        logger.info(
            "AdeClient ready (base=%s, model=%s, job_timeout=%ds, extract_timeout=%ds, "
            "poll_interval=%ds, retries=%d)",
            self.base_url, self.model, self.timeout, self.extract_timeout,
            self.poll_interval, self.retries,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse_pdf(self, pdf_path: str) -> str:
        """
        Upload a PDF and return the Markdown representation.

        Uses the async Parse Jobs API:
          1. POST /parse/jobs  →  job_id
          2. Poll GET /parse/jobs/{job_id} until completed or failed.
          3. Return markdown from response data or S3 output_url.

        Raises:
            FileNotFoundError: if ``pdf_path`` does not exist.
            AdeHTTPError: on HTTP-level failure.
            AdeParseError: if the job fails or has no markdown in response.
            TimeoutError: if the job does not complete within ``self.timeout`` s.
        """
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF not found: {pdf_path}")

        pdf_name = os.path.basename(pdf_path)
        logger.info("ADE parse (jobs API): %s (model=%s)", pdf_name, self.model)
        print(f"    [ADE] Submitting parse job for {pdf_name}...")

        create_payload = self._create_parse_job(pdf_path)

        # Backward compatibility: older ADE responses returned markdown directly
        # in the POST /parse response without async job polling.
        inline_markdown = None
        try:
            inline_markdown = self._extract_markdown_from_result(create_payload)
        except AdeParseError:
            inline_markdown = None
        if inline_markdown:
            logger.info(
                "ADE parse OK (inline response) — %d chars of markdown",
                len(inline_markdown),
            )
            return inline_markdown

        job_id = create_payload.get("job_id")
        if not job_id:
            raise AdeParseError(
                "ADE parse/jobs create response missing 'job_id' and no inline markdown: "
                f"{str(create_payload)[:200]}"
            )
        print(f"    [ADE] Parse job created: {job_id}  (max wait: {self.timeout}s)")

        markdown = self._poll_parse_job(str(job_id))
        logger.info("ADE parse OK — %d chars of markdown", len(markdown))
        return markdown

    def _create_parse_job(self, pdf_path: str) -> dict[str, Any]:
        """
        POST /parse/jobs with the PDF file and return the raw JSON payload.
        Uses ``_with_retries`` to handle transient upload failures.
        """
        def _attempt() -> dict[str, Any]:
            with open(pdf_path, "rb") as fh:
                resp = requests.post(
                    url=f"{self.base_url}/parse/jobs",
                    headers=self._headers,
                    files=[("document", fh)],
                    data={"model": self.model},
                    timeout=_HTTP_TIMEOUT,
                )
            self._check_http(resp, "parse/jobs (create)")
            return self._json_body(resp, "parse/jobs (create)")

        return self._with_retries(_attempt, "parse/jobs (create)")

    def _poll_parse_job(self, job_id: str) -> str:
        """
        Poll GET /parse/jobs/{job_id} until the job reaches ``completed`` or
        ``failed``, or until ``self.timeout`` seconds have elapsed.

        Returns the Markdown string on success.
        Raises ``TimeoutError`` if the deadline is met.
        Raises ``AdeParseError`` if the job fails or the response has no markdown.
        """
        deadline = time.monotonic() + self.timeout
        poll_url = f"{self.base_url}/parse/jobs/{job_id}"
        poll_n = 0

        while time.monotonic() < deadline:
            try:
                resp = requests.get(
                    url=poll_url,
                    headers=self._headers,
                    timeout=_HTTP_TIMEOUT,
                )
                self._check_http(resp, "parse/jobs (poll)")
                body = self._json_body(resp, "parse/jobs (poll)")
            except (requests.Timeout, requests.ConnectionError) as exc:
                logger.warning("ADE poll %d network error (%s) — retrying", poll_n, exc)
                time.sleep(self.poll_interval)
                continue

            status = (body.get("status") or "").lower()
            progress = body.get("progress")  # float 0.0-1.0 or None
            poll_n += 1

            pct = f"{progress * 100:.0f}%" if isinstance(progress, (int, float)) else "?"
            logger.info(
                "ADE parse job %s — poll #%d status=%s progress=%s",
                job_id, poll_n, status, pct,
            )
            print(f"    [ADE] Job {job_id[:12]}...  status={status}  progress={pct}")

            if status == "completed":
                return self._extract_markdown_from_result(body)

            if status == "failed":
                detail = str(body.get("data") or body.get("error") or "")[:300]
                raise AdeParseError(f"ADE parse job {job_id} failed: {detail}")

            # pending / processing — wait and retry
            remaining = deadline - time.monotonic()
            sleep = min(self.poll_interval, max(1, remaining))
            time.sleep(sleep)

        raise TimeoutError(
            f"ADE parse job {job_id} did not complete within {self.timeout}s."
        )

    def _extract_markdown_from_result(self, body: dict[str, Any]) -> str:
        """
        Extract the Markdown string from a completed job response.

        Strategy:
          0. ``body["markdown"]``          — legacy direct parse response.
          1. ``body["data"]["markdown"]`` — small docs (< 1 MB), inline.
          2. ``body["output_url"]``        — large docs, S3 signed URL.
        """
        # ── Path 0: legacy top-level markdown ──────────────────────
        md_top = body.get("markdown")
        if isinstance(md_top, str) and md_top.strip():
            return md_top

        # ── Path 1: inline markdown ──────────────────────────────────────────
        data = body.get("data")
        if isinstance(data, dict):
            md = data.get("markdown")
            if isinstance(md, str) and md.strip():
                return md
            # data is present but no markdown key — try recursive dig
            found = self._dig_for_key(data, "markdown")
            if found:
                return found

        # ── Path 2: S3 signed URL ────────────────────────────────────────────
        output_url = body.get("output_url")
        if output_url:
            logger.info(
                "Markdown not inline — downloading from output_url (%s…)",
                str(output_url)[:60],
            )
            print(f"    [ADE] Downloading markdown from S3 output_url...")
            try:
                dl = requests.get(output_url, timeout=_HTTP_TIMEOUT)
                dl.raise_for_status()
                md = dl.text
                if md.strip():
                    return md
            except requests.RequestException as exc:
                raise AdeParseError(
                    f"Failed to download markdown from output_url: {exc}"
                ) from exc

        raise AdeParseError(
            "ADE parse job completed but response has no markdown inline "
            "and no output_url.  Response keys: "
            + str(list(body.keys()))
        )

    def extract(self, markdown: str, schema: dict[str, Any]) -> dict[str, Any]:
        """
        Send parsed markdown + a JSON schema and return structured data.

        Raises:
            AdeHTTPError: on HTTP-level failure.
            AdeParseError: if the response lacks a ``chapters`` key.
        """
        logger.info("ADE extract: schema keys=%s", list(schema.get("properties", {}).keys()))

        schema_str = json.dumps(schema, ensure_ascii=False)

        _ext_timeout = self.extract_timeout
        logger.info(
            "ADE extract: markdown=%d chars, extract_timeout=%ds",
            len(markdown), _ext_timeout,
        )

        def _attempt() -> dict[str, Any]:
            resp = requests.post(
                url=f"{self.base_url}/extract",
                headers=self._headers,
                files=[("markdown", ("parsed.md", markdown.encode("utf-8"), "text/markdown"))],
                data={"schema": schema_str},
                timeout=_ext_timeout,
            )
            self._check_http(resp, "extract")
            return self._json_body(resp, "extract")

        payload = self._with_retries(_attempt, "extract")

        # ADE wraps results in various envelope shapes — drill to find chapters
        extracted = self._find_payload_with_chapters(payload)
        if extracted is None:
            raise AdeParseError(
                "ADE extract response does not include a payload with 'chapters'."
            )
        logger.info("ADE extract OK — %d chapter(s)", len(extracted.get("chapters", [])))
        return extracted

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _with_retries(self, fn, step: str) -> Any:
        """Execute ``fn`` with exponential back-off retries."""
        last_exc: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                return fn()
            except (requests.Timeout, requests.ConnectionError, AdeHTTPError) as exc:
                last_exc = exc
                if isinstance(exc, AdeHTTPError) and exc.status == 402:
                    print(
                        f"    ⚠️  ADE {step} failed (402: saldo insuficiente). "
                        "No se reintenta automaticamente."
                    )
                    logger.error(
                        "ADE %s failed with 402 (insufficient balance) - not retrying",
                        step,
                    )
                    break
                if not self._is_retriable_error(exc):
                    break
                if attempt == self.retries:
                    break
                wait = _BACKOFF_BASE * attempt  # 30s, 60s, 90s …
                print(
                    f"    ⚠️  ADE {step} attempt {attempt}/{self.retries} failed "
                    f"({type(exc).__name__}: {exc}) — retrying in {wait}s..."
                )
                logger.warning(
                    "ADE %s attempt %d/%d failed (%s) — retrying in %ds",
                    step, attempt, self.retries, exc, wait,
                )
                time.sleep(wait)
        print(
            f"    ❌ ADE {step} failed after {self.retries} attempt(s): "
            f"{type(last_exc).__name__}: {last_exc}"
        )
        raise last_exc  # type: ignore[misc]

    @staticmethod
    def _is_retriable_error(exc: Exception) -> bool:
        if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
            return True
        if isinstance(exc, AdeHTTPError):
            return exc.status in {408, 409, 425, 429, 500, 502, 503, 504}
        return False

    @staticmethod
    def _check_http(resp: requests.Response, step: str) -> None:
        if resp.ok:
            return
        preview = resp.text[:500].replace("\n", " ")
        raise AdeHTTPError(step, resp.status_code, preview)

    @staticmethod
    def _json_body(resp: requests.Response, step: str) -> dict[str, Any]:
        try:
            body = resp.json()
        except ValueError as exc:
            preview = resp.text[:500].replace("\n", " ")
            raise AdeParseError(
                f"ADE {step} returned non-JSON: {preview}"
            ) from exc
        if not isinstance(body, dict):
            raise AdeParseError(
                f"ADE {step} returned unexpected type: {type(body).__name__}"
            )
        return body

    @staticmethod
    def _dig_for_key(payload: Any, key: str) -> str | None:
        """Recursively search nested dicts/lists for a string value at ``key``."""
        if isinstance(payload, dict):
            val = payload.get(key)
            if isinstance(val, str) and val.strip():
                return val
            for sub in payload.values():
                found = AdeClient._dig_for_key(sub, key)
                if found:
                    return found
        elif isinstance(payload, list):
            for item in payload:
                found = AdeClient._dig_for_key(item, key)
                if found:
                    return found
        return None

    @staticmethod
    def _find_payload_with_chapters(payload: Any) -> dict[str, Any] | None:
        """Drill through envelopes to find the dict containing ``chapters``."""
        if isinstance(payload, dict):
            if isinstance(payload.get("chapters"), list):
                return payload
            for k in ("data", "result", "output", "extraction", "extracted", "response"):
                found = AdeClient._find_payload_with_chapters(payload.get(k))
                if found is not None:
                    return found
        elif isinstance(payload, list):
            for item in payload:
                found = AdeClient._find_payload_with_chapters(item)
                if found is not None:
                    return found
        return None

