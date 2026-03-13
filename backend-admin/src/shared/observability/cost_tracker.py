"""
cost_tracker.py — Thread-safe Gemini API cost accumulator
==========================================================
Tracks prompt + output tokens per model across the full pipeline run
and computes an estimated USD cost at the end.

Usage
-----
    from utils.cost_tracker import record_usage, print_cost_summary, reset

    # In each agent, after a successful generate_content() call:
    if hasattr(response, "usage_metadata") and response.usage_metadata:
        record_usage(
            model_name,
            response.usage_metadata.prompt_token_count  or 0,
            response.usage_metadata.candidates_token_count or 0,
        )

    # At the end of the pipeline:
    print_cost_summary()

    # At the start of a new pipeline run (optional, resets per-request totals):
    reset()

Pricing reference (USD per 1 million tokens, approximate)
----------------------------------------------------------
Gemini 2.5 Flash
  Input  : $0.30  (user-stated)
  Output : $1.00  (Google AI standard tier)

Gemini 2.5 Pro  — two tiers, selected by prompt_token_count
  ≤ 200k context  Input: $0.625   Output: $5.00
  > 200k context  Input: $1.25    Output: $7.50

Unknown models fall back to the Pro pricing as a conservative upper bound.
"""

import threading

# ─── Pricing table ────────────────────────────────────────────────────────────
_PRICES: dict[str, dict] = {
    "gemini-2.5-flash": {
        "input":  0.30,    # $/1M tokens — stated by user
        "output": 1.00,    # $/1M tokens — Google AI standard pricing
    },
    "gemini-2.5-pro": {
        # Pro billing splits at 200 000 context tokens
        "input_low":      0.625,    # ≤ 200k  $/1M
        "output_low":     5.00,
        "input_high":     1.25,     # > 200k  $/1M
        "output_high":    7.50,
        "ctx_threshold":  200_000,
    },
}

_DEFAULT_KEY = "gemini-2.5-pro"   # fallback for unknown model names

# ─── Internal state ───────────────────────────────────────────────────────────
_lock: threading.Lock = threading.Lock()

# { normalised_model_key: {"input": int, "output": int, "calls": int} }
_totals: dict[str, dict[str, int]] = {}


# ─── Public API ───────────────────────────────────────────────────────────────

def record_usage(model_name: str, prompt_tokens: int, output_tokens: int) -> None:
    """
    Thread-safe accumulator: adds one API call's token counts.

    Safe to call even when values are 0 or model_name is unknown.
    Never raises — cost tracking must never crash the pipeline.
    """
    try:
        pt = int(prompt_tokens  or 0)
        ot = int(output_tokens  or 0)
        key = _normalise(model_name)

        with _lock:
            if key not in _totals:
                _totals[key] = {"input": 0, "output": 0, "calls": 0}
            _totals[key]["input"]  += pt
            _totals[key]["output"] += ot
            _totals[key]["calls"]  += 1
    except Exception:
        pass  # never let cost tracking crash the pipeline


def reset() -> None:
    """Clears all accumulated totals. Call once at the start of each pipeline run."""
    with _lock:
        _totals.clear()


def print_cost_summary() -> None:
    """
    Prints a per-model and total cost breakdown to stdout.
    Costs are in USD with 6 decimal places.
    """
    with _lock:
        snapshot = {k: dict(v) for k, v in _totals.items()}

    sep = "=" * 68
    print(f"\n{sep}")
    print("💰  GEMINI API COST SUMMARY (estimated, USD)")
    print(sep)

    if not snapshot:
        print("   No API calls recorded.")
        print(sep)
        return

    grand_in   = 0
    grand_out  = 0
    grand_calls = 0
    grand_cost  = 0.0

    for key in sorted(snapshot):
        counts = snapshot[key]
        in_tok  = counts["input"]
        out_tok = counts["output"]
        calls   = counts["calls"]

        c_in, c_out = _compute_cost(key, in_tok, out_tok)
        total_cost   = c_in + c_out

        grand_in    += in_tok
        grand_out   += out_tok
        grand_calls += calls
        grand_cost  += total_cost

        print(
            f"  {key:<26}  {calls:>5} calls  "
            f"in={in_tok:>12,}  out={out_tok:>10,}  "
            f"${total_cost:.6f}"
        )

    print("-" * 68)
    print(
        f"  {'TOTAL':<26}  {grand_calls:>5} calls  "
        f"in={grand_in:>12,}  out={grand_out:>10,}  "
        f"${grand_cost:.6f}"
    )
    print(sep)
    print("  ⚠️  Prices are approximate. Verify at https://ai.google.dev/pricing")
    print(sep)


def get_totals() -> dict:
    """Returns a deep copy of the current accumulated totals (for testing/logging)."""
    with _lock:
        return {k: dict(v) for k, v in _totals.items()}


def get_cost_summary() -> dict:
    """
    Returns an aggregated cost snapshot suitable for persistence in run payloads.
    """
    with _lock:
        snapshot = {k: dict(v) for k, v in _totals.items()}

    models: list[dict[str, float | int | str]] = []
    grand_input_tokens = 0
    grand_output_tokens = 0
    grand_calls = 0
    grand_cost_usd = 0.0

    for model_key in sorted(snapshot):
        counts = snapshot[model_key]
        input_tokens = int(counts.get("input") or 0)
        output_tokens = int(counts.get("output") or 0)
        calls = int(counts.get("calls") or 0)
        cost_input_usd, cost_output_usd = _compute_cost(model_key, input_tokens, output_tokens)
        total_cost_usd = cost_input_usd + cost_output_usd

        grand_input_tokens += input_tokens
        grand_output_tokens += output_tokens
        grand_calls += calls
        grand_cost_usd += total_cost_usd

        models.append(
            {
                "model": model_key,
                "calls": calls,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost_input_usd": round(float(cost_input_usd), 8),
                "cost_output_usd": round(float(cost_output_usd), 8),
                "total_cost_usd": round(float(total_cost_usd), 8),
            }
        )

    return {
        "currency": "USD",
        "models": models,
        "total_calls": grand_calls,
        "total_input_tokens": grand_input_tokens,
        "total_output_tokens": grand_output_tokens,
        "total_cost_usd": round(float(grand_cost_usd), 8),
    }


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _normalise(model_name: str) -> str:
    """Maps any model name string to a key in _PRICES."""
    name = (model_name or "").lower().strip()
    if "flash" in name:
        return "gemini-2.5-flash"
    if "pro" in name:
        return "gemini-2.5-pro"
    return _DEFAULT_KEY


def _compute_cost(key: str, input_tokens: int, output_tokens: int) -> tuple[float, float]:
    """Returns (cost_input_usd, cost_output_usd) for the given token counts."""
    prices = _PRICES.get(key, _PRICES[_DEFAULT_KEY])

    if "ctx_threshold" in prices:
        # Pro: two-tier pricing — use actual input token count to select tier
        if input_tokens > prices["ctx_threshold"]:
            p_in  = prices["input_high"]
            p_out = prices["output_high"]
        else:
            p_in  = prices["input_low"]
            p_out = prices["output_low"]
    else:
        p_in  = prices["input"]
        p_out = prices["output"]

    return (input_tokens / 1_000_000) * p_in, (output_tokens / 1_000_000) * p_out
