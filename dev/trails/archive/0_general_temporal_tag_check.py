# conda activate trails
# cd C:\Users\terlouw_t\Documents\Projects\premise_trails\dev\trails
# Codex CLI first, then API fallback:
# python 0_general_temporal_tag_check.py 
# Before API use in PowerShell:
# $env:OPENAI_API_KEY=""

# Note, this has not been tested due to not sufficient credits...
"""
TRAILS temporal-tag checker from temporal_distributions.csv only

What this script does
1) Loads temporal_distributions.csv (or xlsx).
2) Sends each row to Codex / OpenAI API in batches.
3) Asks whether the CURRENT temporal_tag is realistic based on:
   - activity name
   - reference product
   - optional CPC / ISIC / category columns if present
   - current tag notes if present
4) Writes a review CSV with:
   - codex_current_tag_realistic
   - codex_suggested_temporal_tag
   - codex_confidence
   - codex_main_reason
   - codex_tag_reasoning
   - codex_batch_error

Requirements:
  pip install pandas numpy openpyxl

Optional for OpenAI API validation:
  pip install openai
  set OPENAI_API_KEY

Optional for Codex CLI validation:
  - Install Codex CLI separately
  - Make sure the executable is on PATH, or pass --codex-exe
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

# ----------------------------
# CONFIG
# ----------------------------
DEFAULT_CSV = r"temporal_distributions.csv"
DEFAULT_OUT = r"temporal_tag_review.csv"

DEFAULT_USE_CODEX = True
DEFAULT_CODEX_MODEL = "gpt-5.4"
DEFAULT_API_MODELS = ["gpt-5.4", "gpt-5-mini"]
DEFAULT_API_TIMEOUT = 180

VALID_TEMPORAL_TAGS = [
    "throughput_process",
    "stock_asset",
    "biomass_growth",
    "end_of_life",
    "maintenance",
    "market",
    "",
]

try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None


REVIEW_OUTPUT_COLUMNS = [
    "codex_current_tag_realistic",
    "codex_suggested_temporal_tag",
    "codex_confidence",
    "codex_main_reason",
    "codex_tag_reasoning",
    "codex_batch_error",
]


def sanitize_text_for_csv(x: Any) -> Any:
    if x is None:
        return None

    if isinstance(x, (int, float, bool, np.integer, np.floating)):
        if isinstance(x, float) and np.isnan(x):
            return None
        return x

    s = str(x)
    s = s.replace("\r\n", " | ").replace("\n", " | ").replace("\r", " | ").replace("\t", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def sanitize_dataframe_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].map(sanitize_text_for_csv)
    return out


def fmt_time(sec: float) -> str:
    sec = max(0.0, float(sec))
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h:d}h{m:02d}m{s:02d}s"
    if m:
        return f"{m:d}m{s:02d}s"
    return f"{s:d}s"


def to_jsonable(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (np.integer,)):
        return int(x)
    if isinstance(x, (np.floating,)):
        if np.isnan(x):
            return None
        return float(x)
    if isinstance(x, (np.bool_,)):
        return bool(x)
    if isinstance(x, float):
        if np.isnan(x):
            return None
        return x
    if isinstance(x, tuple):
        return [to_jsonable(v) for v in x]
    if isinstance(x, list):
        return [to_jsonable(v) for v in x]
    if isinstance(x, dict):
        return {str(k): to_jsonable(v) for k, v in x.items()}
    return x


def load_temporal_distributions(csv_path: str) -> pd.DataFrame:
    fp = Path(csv_path)
    if not fp.exists():
        raise FileNotFoundError(csv_path)

    if fp.suffix.lower() in [".xlsx", ".xls", ".xlsm"]:
        df = pd.read_excel(fp)
    else:
        try:
            df = pd.read_csv(fp, encoding="utf-8-sig")
        except UnicodeDecodeError:
            df = pd.read_csv(fp, encoding="latin1")

    return df


def initialize_review_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in REVIEW_OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df


def resolve_codex_executable(user_value: Optional[str] = None) -> Optional[str]:
    candidates: List[str] = []

    if user_value:
        candidates.append(user_value)

    env_value = os.environ.get("CODEX_EXE")
    if env_value:
        candidates.append(env_value)

    candidates.extend(["codex", "codex.cmd", "codex.exe"])

    for cand in candidates:
        if not cand:
            continue

        p = Path(cand)
        if p.exists():
            return str(p)

        resolved = shutil.which(cand)
        if resolved:
            return resolved

    return None


def codex_cli_available(codex_exe: Optional[str]) -> bool:
    return codex_exe is not None


def openai_api_available() -> bool:
    # Disable API entirely unless explicitly enabled
    return False


def build_review_payload_for_row(row: pd.Series, row_index: int) -> Dict[str, Any]:
    payload = {
        "row_index": int(row_index),
        "name": row.get("name", ""),
        "reference_product": row.get("reference product", ""),
        "current_temporal_tag": row.get("temporal_tag", ""),
        "tag_confidence": row.get("tag_confidence", None),
        "tag_notes": row.get("tag_notes", ""),
        "param_notes": row.get("param_notes", ""),
        "ISIC_rev4_ecoinvent": row.get("ISIC rev.4 ecoinvent", ""),
        "CPC": row.get("CPC", ""),
        "EcoSpold01Categories": row.get("EcoSpold01Categories", ""),
        "lifetime": row.get("lifetime", None),
        "age_distribution_type": row.get("age distribution type", None),
        "valid_temporal_tags": VALID_TEMPORAL_TAGS,
    }
    return to_jsonable(payload)


def build_tag_review_prompt(batch_payload: List[Dict[str, Any]]) -> str:
    schema_example = {
        "results": [
            {
                "row_index": 0,
                "current_tag_realistic": True,
                "suggested_temporal_tag": "stock_asset",
                "confidence": "high",
                "main_reason": "The activity appears to represent a durable capital good, so stock_asset is plausible.",
                "tag_reasoning": "short sentence"
            }
        ]
    }

    safe_batch_payload = to_jsonable(batch_payload)

    return f"""
You are reviewing whether the CURRENT temporal_tag assigned to TRAILS rows is realistic.

Allowed temporal_tag values:
- throughput_process
- stock_asset
- biomass_growth
- end_of_life
- maintenance
- market
- "" (empty string; only if no explicit temporal class is justified)

Interpret the tags as follows:

throughput_process:
Direct process throughput, ordinary production/transformation activities, short-lived operating flows, or rows where no special stock, market, biomass growth, maintenance, or end-of-life timing class is justified.

stock_asset:
Durable capital goods, infrastructure, machinery, installations, equipment, vehicles, buildings, technical systems, or other long-lived stock assets.

biomass_growth:
Biogenic growth / regrowth / land occupation / biomass accumulation processes with time-dependent growth dynamics.

end_of_life:
Waste treatment, dismantling, disposal, recycling, incineration, landfill, decommissioning, or other terminal treatment activities.

maintenance:
Maintenance, repair, replacement, servicing, refurbishment, or recurring upkeep activities linked to assets.

market:
Market / market group / supply-mix / pooling activities that mainly represent aggregation or supply composition rather than a specific transforming process.

Decision task:
For each row, judge whether the CURRENT temporal_tag is realistic given:
- name
- reference product
- CPC / ISIC / categories if available
- current tag notes if available
- current parameter notes if available

Important rules:
1) First infer what kind of activity the row represents.
2) Then judge whether the CURRENT temporal_tag fits that activity type.
3) Only suggest a different temporal_tag if the current one is not realistic.
4) Be conservative.
5) Do not overuse stock_asset.
6) Use throughput_process as the default for ordinary transforming processes when no more special class is clearly justified.
7) Use market only for genuine market / supply aggregation rows.
8) Use maintenance only for genuine upkeep / servicing / replacement rows.
9) Use end_of_life only for terminal disposal / recycling / waste treatment rows.
10) Use biomass_growth only for genuine biomass growth / regrowth / accumulation rows.

Output requirements:
- Every input row must appear exactly once.
- Return EXACTLY one JSON object with a top-level "results" list.
- Do not use markdown fences.
- Do not include text before or after the JSON.
- suggested_temporal_tag must be one of the allowed tags above.
- current_tag_realistic must be true or false.
- confidence must be one of: low, medium, high.

Required JSON shape:
{json.dumps(schema_example, ensure_ascii=False, indent=2)}

Rows:
{json.dumps(safe_batch_payload, ensure_ascii=False)}
""".strip()


def parse_batch_output(text: str) -> Dict[int, Dict[str, Any]]:
    raw = (text or "").strip()

    if not raw:
        raise ValueError("Model returned empty text.")

    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError(f"Model output is not valid JSON.\nRaw output:\n{raw[:4000]}")
        data = json.loads(raw[start:end + 1])

    if not isinstance(data, dict) or "results" not in data or not isinstance(data["results"], list):
        raise ValueError(f"Output must be a JSON object with a 'results' list.\nParsed output:\n{data}")

    out: Dict[int, Dict[str, Any]] = {}
    for item in data["results"]:
        if not isinstance(item, dict):
            continue
        idx = item.get("row_index")
        if idx is None:
            continue
        out[int(idx)] = item
    return out


def run_codex_batch(
    batch_payload: List[Dict[str, Any]],
    *,
    codex_exe: str,
    codex_model: str,
    codex_timeout: int,
) -> Dict[int, Dict[str, Any]]:
    prompt = build_tag_review_prompt(batch_payload)

    env = os.environ.copy()
    env.setdefault("NO_COLOR", "1")
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    cmd = [codex_exe, "exec", "--model", codex_model, "-"]

    completed = subprocess.run(
        cmd,
        input=prompt,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        timeout=codex_timeout,
        shell=False,
    )

    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()

    if completed.returncode != 0:
        raise RuntimeError(
            "Codex failed.\n"
            f"Return code: {completed.returncode}\n"
            f"STDOUT:\n{stdout or '[empty]'}\n\n"
            f"STDERR:\n{stderr or '[empty]'}"
        )

    if not stdout:
        raise RuntimeError(f"Codex returned empty stdout.\nSTDERR:\n{stderr or '[empty]'}")

    return parse_batch_output(stdout)


def extract_text_from_response(resp: Any) -> str:
    output_text = getattr(resp, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    text_parts: List[str] = []
    output = getattr(resp, "output", None)
    if output:
        for item in output:
            content = getattr(item, "content", None) or []
            for c in content:
                txt = getattr(c, "text", None)
                if isinstance(txt, str):
                    text_parts.append(txt)

    if text_parts:
        return "\n".join(text_parts).strip()

    return str(resp).strip()


def run_openai_api_batch(
    batch_payload: List[Dict[str, Any]],
    *,
    api_models: Sequence[str],
    api_timeout: int,
) -> Dict[int, Dict[str, Any]]:
    if not openai_api_available():
        raise RuntimeError("OpenAI API not available. Install `openai` and set OPENAI_API_KEY.")

    client = OpenAI(timeout=api_timeout)
    prompt = build_tag_review_prompt(batch_payload)
    last_err: Optional[Exception] = None

    for model_name in api_models:
        try:
            resp = client.responses.create(
                model=model_name,
                input=prompt,
            )
            text = extract_text_from_response(resp)
            return parse_batch_output(text)
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"All API fallback models failed. Last error: {last_err}")


def run_llm_batch_with_fallback(
    batch_payload: List[Dict[str, Any]],
    *,
    prefer_codex_cli: bool,
    codex_exe: Optional[str],
    codex_model: str,
    codex_timeout: int,
    api_models: Sequence[str],
    api_timeout: int,
) -> Dict[int, Dict[str, Any]]:
    cli_error: Optional[Exception] = None

    if prefer_codex_cli and codex_exe:
        try:
            return run_codex_batch(
                batch_payload,
                codex_exe=codex_exe,
                codex_model=codex_model,
                codex_timeout=codex_timeout,
            )
        except Exception as e:
            cli_error = e

    try:
        return run_openai_api_batch(
            batch_payload,
            api_models=api_models,
            api_timeout=api_timeout,
        )
    except Exception as api_err:
        if cli_error is not None:
            raise RuntimeError(
                f"Both Codex CLI and OpenAI API fallback failed.\nCLI error: {cli_error}\nAPI error: {api_err}"
            )
        raise


def apply_review_results(
    df: pd.DataFrame,
    results_by_idx: Dict[int, Dict[str, Any]],
    *,
    batch_error: Optional[str] = None,
) -> None:
    for idx, result in results_by_idx.items():
        if idx not in df.index:
            continue

        df.at[idx, "codex_current_tag_realistic"] = result.get("current_tag_realistic")
        df.at[idx, "codex_suggested_temporal_tag"] = result.get("suggested_temporal_tag")
        df.at[idx, "codex_confidence"] = result.get("confidence")
        df.at[idx, "codex_main_reason"] = result.get("main_reason")
        df.at[idx, "codex_tag_reasoning"] = result.get("tag_reasoning")
        df.at[idx, "codex_batch_error"] = batch_error


def run_tag_review(
    out_df: pd.DataFrame,
    *,
    use_codex: bool,
    codex_exe: Optional[str],
    codex_batch_size: int,
    codex_max_rows: Optional[int],
    codex_model: str,
    codex_timeout: int,
    codex_fail_fast: bool,
    print_every_seconds: float,
    prefer_codex_cli: bool = False,
    api_models: Optional[Sequence[str]] = None,
    api_timeout: int = DEFAULT_API_TIMEOUT,
) -> pd.DataFrame:
    out_df = initialize_review_columns(out_df)

    if not use_codex:
        print("Model review disabled.", flush=True)
        return out_df

    if api_models is None:
        api_models = DEFAULT_API_MODELS

    if prefer_codex_cli and not codex_cli_available(codex_exe):
        print("WARNING: Codex CLI requested but executable not found. Falling back to API only.", flush=True)

    if not codex_cli_available(codex_exe):
        msg = "Codex CLI not available. Cannot run review."
        print(msg, flush=True)
        out_df["codex_batch_error"] = msg
        return out_df

    target_indices = list(out_df.index)

    if codex_max_rows is not None:
        target_indices = target_indices[: max(0, int(codex_max_rows))]

    if not target_indices:
        print("Review skipped: no rows selected.", flush=True)
        return out_df

    print(
        f"Starting temporal-tag review for {len(target_indices)} rows "
        f"(batch_size={codex_batch_size}, prefer_codex_cli={prefer_codex_cli}, "
        f"codex_exe={codex_exe}, api_models={list(api_models)})...",
        flush=True,
    )

    t0 = time.time()
    last_print = t0
    done = 0
    batch_size = max(1, codex_batch_size)

    for start in range(0, len(target_indices), batch_size):
        batch_indices = target_indices[start:start + batch_size]
        batch_payload = [build_review_payload_for_row(out_df.loc[idx], idx) for idx in batch_indices]

        try:
            results_by_idx = run_llm_batch_with_fallback(
                batch_payload,
                prefer_codex_cli=prefer_codex_cli,
                codex_exe=codex_exe,
                codex_model=codex_model,
                codex_timeout=codex_timeout,
                api_models=api_models,
                api_timeout=api_timeout,
            )

            apply_review_results(out_df, results_by_idx, batch_error=None)

            missing = [idx for idx in batch_indices if idx not in results_by_idx]
            for idx in missing:
                out_df.at[idx, "codex_batch_error"] = "Model returned no result for this row."

        except Exception as e:
            err = sanitize_text_for_csv(str(e))[:2000]
            for idx in batch_indices:
                out_df.at[idx, "codex_batch_error"] = err

            if codex_fail_fast:
                raise

        done += len(batch_indices)
        now = time.time()
        if (now - last_print >= print_every_seconds) or (done == len(target_indices)):
            elapsed = now - t0
            rate = elapsed / max(done, 1)
            eta = (len(target_indices) - done) * rate
            pct = 100.0 * done / max(len(target_indices), 1)
            msg = (
                f"[Tag review {done}/{len(target_indices)} | {pct:5.1f}%] "
                f"elapsed={fmt_time(elapsed)} avg={rate:.3f}s/row ETA={fmt_time(eta)}"
            )
            print("\r" + msg.ljust(130), end="", flush=True)
            last_print = now

    print()
    return out_df


def run(
    csv_path: str = DEFAULT_CSV,
    out_path: str = DEFAULT_OUT,
    *,
    print_every_seconds: float = 5.0,
    use_codex: bool = DEFAULT_USE_CODEX,
    codex_exe: Optional[str] = None,
    codex_batch_size: int = 50,
    codex_max_rows: Optional[int] = None,
    codex_model: str = DEFAULT_CODEX_MODEL,
    codex_timeout: int = 300,
    codex_fail_fast: bool = False,
    prefer_codex_cli: bool = False,
    api_models: Optional[Sequence[str]] = None,
    api_timeout: int = DEFAULT_API_TIMEOUT,
) -> pd.DataFrame:
    if api_models is None:
        api_models = DEFAULT_API_MODELS

    t0 = time.time()
    df = load_temporal_distributions(csv_path)
    print(f"Loaded {len(df):,} temporal-distribution rows in {fmt_time(time.time() - t0)}", flush=True)

    out_df = df.copy()

    out_df = run_tag_review(
        out_df,
        use_codex=use_codex,
        codex_exe=codex_exe,
        codex_batch_size=codex_batch_size,
        codex_max_rows=codex_max_rows,
        codex_model=codex_model,
        codex_timeout=codex_timeout,
        codex_fail_fast=codex_fail_fast,
        print_every_seconds=print_every_seconds,
        prefer_codex_cli=prefer_codex_cli,
        api_models=api_models,
        api_timeout=api_timeout,
    )

    out_df["tag_changed_by_codex"] = (
        out_df["temporal_tag"].fillna("").astype(str).str.strip()
        != out_df["codex_suggested_temporal_tag"].fillna("").astype(str).str.strip()
    )

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    out_df_export = sanitize_dataframe_for_csv(out_df)
    out_df_export.to_csv(
        out_path,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_ALL,
    )
    return out_df_export


def main():
    print("Running temporal tag checker with default configuration...", flush=True)

    codex_exe = resolve_codex_executable()

    df = run(
            prefer_codex_cli=True,
            codex_exe=resolve_codex_executable(),
            use_codex=True,
            codex_batch_size=100,
            api_models=[],   # 🔴 critical
        )

    print(f"Wrote: {DEFAULT_OUT} ({len(df)} rows)", flush=True)


if __name__ == "__main__":
    main()