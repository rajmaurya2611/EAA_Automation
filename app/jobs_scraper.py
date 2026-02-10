#!/usr/bin/env python3
"""
job_scraper.py — Unstop Jobs Scraper (public API)

- Fetches job opportunities from:
  https://unstop.com/api/public/opportunity/search-result?opportunity=jobs&page=1&per_page=18

- Extracts (logic unchanged):
  - salary as numbers only (no currency symbol, no commas, no "/year")
    Rules:
      - if show_salary == 0 OR not_disclosed == True OR missing -> "Not disclosed"
      - if only max present -> "<max>"
      - if both present -> "<min>-<max>"
      - if only min present -> "<min>"
  - work_mode: in_office | wfh | hybrid | on_field | None
  - details: extracted from HTML in "details" (if present)

- Output changes:
  - Column rename/drop exactly per your list (only keep the fields you listed)
  - Add: status = 1 (always)
  - Add: order = 0 (always)
  - Writes: CSV + JSON (single file, not JSONL)

Additional tweaks requested:
  - job_location: if empty -> "Online"
  - application_deadline: IST date only (YYYY-MM-DD)

No artificial delays; only backoff on errors / rate-limits via urllib3 Retry.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://unstop.com/api/public/opportunity/search-result"

# -----------------------------
# HTML parsing helpers (stdlib-only)
# -----------------------------
_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")


def _strip_tags(html: str) -> str:
    txt = unescape(html or "")
    txt = _TAG_RE.sub(" ", txt)
    txt = _SPACE_RE.sub(" ", txt).strip()
    return txt


# -----------------------------
# Formatting helpers
# -----------------------------
def _safe_join(xs: Iterable[str], sep: str = ", ") -> str:
    xs2 = [str(x).strip() for x in xs if str(x).strip()]
    return sep.join(xs2)


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def build_salary_numeric(job_detail: dict | None) -> str:
    """
    Salary string:
    - numbers only
    - no ₹, no commas, no "/year" or unit text

    Output examples:
      "Not disclosed"
      "900000"
      "400000-900000"
    """
    jd = job_detail or {}
    show_salary = jd.get("show_salary", None)
    not_disclosed = jd.get("not_disclosed", None)

    # Explicitly hidden
    if show_salary in (0, "0", False, "false") or str(show_salary).lower() == "false" or not_disclosed is True:
        return "Not disclosed"

    min_sal = _to_int(jd.get("min_salary"))
    max_sal = _to_int(jd.get("max_salary"))

    if min_sal is not None and max_sal is not None:
        return f"{min_sal}-{max_sal}"
    if max_sal is not None and min_sal is None:
        return f"{max_sal}"
    if min_sal is not None and max_sal is None:
        return f"{min_sal}"
    return "Not disclosed"


def build_work_mode(job_detail: dict | None, region: str | None = None) -> Optional[str]:
    """Normalizes to: in_office | wfh | hybrid | on_field | <raw> | None"""
    jd = job_detail or {}
    t = (jd.get("type") or "").strip().lower()

    if t in {"in_office", "in-office", "office", "in office"}:
        return "in_office"
    if t == "hybrid":
        return "hybrid"
    if t in {"remote", "wfh", "work_from_home", "work-from-home"}:
        return "wfh"
    if t in {"on_field", "field", "on-field"}:
        return "on_field"
    if not t and (region or "").strip().lower() == "online":
        return "wfh"
    return t or None


def format_ist_date(value: Any) -> str:
    """
    Convert ISO8601 like '2026-02-01T00:00:00+05:30' to '2026-02-01' (IST date).
    If already 'YYYY-MM-DD', returns it.
    If empty/unparseable, returns ''.
    """
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    # Fast path for 'YYYY-MM-DD...'
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    try:
        dt = datetime.fromisoformat(s)  # supports +05:30 in modern Python
        return dt.date().isoformat()
    except Exception:
        return s[:10] if len(s) >= 10 else ""


# -----------------------------
# HTTP client with retry
# -----------------------------
def make_session(timeout_s: int = 25) -> Tuple[requests.Session, int]:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session, timeout_s


# -----------------------------
# Scraper core
# -----------------------------
@dataclass
class ScrapeConfig:
    per_page: int = 18
    max_pages: int = 0  # 0 = auto until empty
    out_prefix: str = "unstop_jobs"
    extra_params: Optional[Dict[str, Any]] = None  # keep None for "no filters"


def fetch_page(
    session: requests.Session,
    timeout_s: int,
    page: int,
    per_page: int,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    params = {
        "opportunity": "jobs",
        "page": page,
        "per_page": per_page,
    }
    if extra_params:
        for k, v in extra_params.items():
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            params[k] = v

    headers = {
        "accept": "application/json, text/plain, */*",
        "user-agent": "Mozilla/5.0 (compatible; UnstopScraper/1.0; +https://unstop.com)",
        "referer": "https://unstop.com/",
    }
    r = session.get(BASE_URL, params=params, headers=headers, timeout=timeout_s)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:500]}")
    try:
        return r.json()
    except Exception as e:
        raise RuntimeError(f"Failed to parse JSON: {e}. Body head: {r.text[:500]}") from e


def extract_row(item: Dict[str, Any]) -> Dict[str, Any]:
    """Extraction logic stays the same; only: details extracted from HTML (plain text)."""
    org = item.get("organisation") or item.get("organization") or {}
    job_detail = item.get("jobDetail") or item.get("job_detail") or {}

    # Locations: list of dicts
    loc_dicts = item.get("locations") or []
    loc_cities, loc_states, loc_countries = [], [], []
    for ld in loc_dicts:
        if isinstance(ld, dict):
            if ld.get("city"):
                loc_cities.append(ld["city"])
            if ld.get("state"):
                loc_states.append(ld["state"])
            if ld.get("country"):
                loc_countries.append(ld["country"])

    # Skills
    required_skills = item.get("required_skills") or item.get("skills") or []
    skill_names = []
    for s in required_skills:
        if isinstance(s, dict):
            skill_names.append(s.get("skill_name") or s.get("skill") or "")

    # Eligibility tags
    filters = item.get("filters") or []
    eligibility_tags = []
    for f in filters:
        if isinstance(f, dict) and f.get("name"):
            eligibility_tags.append(f["name"])

    # Salary + work mode
    salary = build_salary_numeric(job_detail)
    work_mode = build_work_mode(job_detail, region=item.get("region"))

    # Details extraction
    details_html = item.get("details") or ""
    details_text = _strip_tags(details_html) if details_html else ""

    row = {
        "id": item.get("id"),
        "title": item.get("title"),
        "type": item.get("type"),
        "subtype": item.get("subtype"),
        "status": item.get("status"),  # from API (ignored later; you want status=1)
        "company_id": org.get("id") or item.get("organization_id"),
        "company": org.get("name"),
        "updated_at": item.get("updated_at"),
        "end_date": item.get("end_date"),
        "region": item.get("region"),
        "location_cities": _safe_join(loc_cities),
        "location_states": _safe_join(loc_states),
        "location_countries": _safe_join(loc_countries),
        "work_mode": work_mode,
        "timing": job_detail.get("timing"),
        "paid_unpaid": job_detail.get("paid_unpaid"),
        "pay_in": job_detail.get("pay_in"),
        "salary": salary,
        "skills": _safe_join(skill_names),
        "eligibility": _safe_join(eligibility_tags),
        "details": details_text,
        "viewsCount": item.get("viewsCount"),
        "registerCount": item.get("registerCount"),
        "public_url": item.get("public_url"),
        "seo_url": item.get("seo_url"),
        "short_url": item.get("short_url"),
        "short_id": item.get("short_id"),
        "logoUrl2": item.get("logoUrl2") or org.get("logoUrl2") or org.get("logoUrl"),
    }
    return row


# -----------------------------
# Column remap (your contract)
# -----------------------------
COLUMN_MAP = {
    # ✅ FIX: job_id must come from API "id"
    "id": "job_id",
    # ✅ NEW: keep title separately as job_title
    "title": "job_title",
    "company": "company_name",
    "company_id": "company_id",
    "type": "type",
    "subtype": "subtype",
    "work_mode": "work_mode",
    "salary": "salary_range",
    "timing": "timing",
    "paid_unpaid": "paid_unpaid",
    "pay_in": "pay_in",
    "location_cities": "job_location",
    "region": "region",
    "end_date": "application_deadline",
    "updated_at": "updated_at",
    "skills": "required_skills",
    "eligibility": "eligibility_criteria",
    "viewsCount": "viewsCount",
    "registerCount": "registerCount",
    "seo_url": "application_url",
    "short_url": "short_url",
    "public_url": "public_url",
    "short_id": "short_id",
    "logoUrl2": "logo_url",
    "details": "details",
}

OUTPUT_COLUMNS = [
    "job_id",
    "job_title",
    "company_name",
    "company_id",
    "type",
    "subtype",
    "status",  # default 1
    "work_mode",
    "salary_range",
    "timing",
    "paid_unpaid",
    "pay_in",
    "job_location",
    "region",
    "application_deadline",
    "updated_at",
    "required_skills",
    "eligibility_criteria",
    "viewsCount",
    "registerCount",
    "application_url",
    "short_url",
    "public_url",
    "short_id",
    "logo_url",
    "details",
    "order",  # default 0
]


def remap_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for src_key, dst_key in COLUMN_MAP.items():
        if src_key in row:
            out[dst_key] = row.get(src_key)

    # Defaults as requested
    out["status"] = 1
    out["order"] = 0

    # ✅ job_location default
    jl = (out.get("job_location") or "").strip()
    if not jl:
        out["job_location"] = "Online"

    # ✅ application_deadline: IST date only YYYY-MM-DD
    out["application_deadline"] = format_ist_date(out.get("application_deadline"))

    # Ensure stable schema
    for k in OUTPUT_COLUMNS:
        out.setdefault(k, "")
    return out


def scrape_jobs(cfg: ScrapeConfig) -> List[Dict[str, Any]]:
    session, timeout_s = make_session()
    rows: List[Dict[str, Any]] = []
    page = 1

    while True:
        if cfg.max_pages and page > cfg.max_pages:
            break

        payload = fetch_page(
            session=session,
            timeout_s=timeout_s,
            page=page,
            per_page=cfg.per_page,
            extra_params=cfg.extra_params,
        )
        data_block = payload.get("data") or {}
        items = data_block.get("data") or []
        if not items:
            break

        for item in items:
            if isinstance(item, dict):
                extracted = extract_row(item)
                rows.append(remap_row(extracted))

        page += 1

    return rows


# -----------------------------
# Writers
# -----------------------------
def _timestamp_str() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def write_json(rows: List[Dict[str, Any]], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def write_csv(rows: List[Dict[str, Any]], path: str) -> None:
    if not rows:
        with open(path, "w", encoding="utf-8") as f:
            f.write("")
        return

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in OUTPUT_COLUMNS})


# -----------------------------
# CLI
# -----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scrape Unstop jobs to CSV + JSON.")
    p.add_argument("--per-page", type=int, default=18)
    p.add_argument("--max-pages", type=int, default=0, help="0 = until empty")
    p.add_argument("--out-prefix", type=str, default="unstop_jobs")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    cfg = ScrapeConfig(
        per_page=max(1, int(args.per_page)),
        max_pages=max(0, int(args.max_pages)),
        out_prefix=str(args.out_prefix).strip() or "unstop_jobs",
        extra_params=None,  # no filters
    )

    rows = scrape_jobs(cfg)
    ts = _timestamp_str()
    csv_path = f"{cfg.out_prefix}_{ts}.csv"
    json_path = f"{cfg.out_prefix}_{ts}.json"

    write_csv(rows, csv_path)
    write_json(rows, json_path)

    print(f"[OK] rows={len(rows)}")
    print(f"[OK] wrote CSV → {csv_path}")
    print(f"[OK] wrote JSON → {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
