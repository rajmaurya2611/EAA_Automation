# app/job_platforms/unstop.py
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Tuple, Set

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://unstop.com/api/public/opportunity/search-result"

_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")


def _strip_tags(html: str) -> str:
    txt = unescape(html or "")
    txt = _TAG_RE.sub(" ", txt)
    txt = _SPACE_RE.sub(" ", txt).strip()
    return txt


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
    jd = job_detail or {}
    show_salary = jd.get("show_salary", None)
    not_disclosed = jd.get("not_disclosed", None)

    if (
        show_salary in (0, "0", False, "false")
        or str(show_salary).lower() == "false"
        or not_disclosed is True
    ):
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
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    try:
        dt = datetime.fromisoformat(s)
        return dt.date().isoformat()
    except Exception:
        return s[:10] if len(s) >= 10 else ""


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


@dataclass
class ScrapeConfig:
    per_page: int = 18
    max_pages: int = 0
    extra_params: Optional[Dict[str, Any]] = None


def fetch_page(
    session: requests.Session,
    timeout_s: int,
    page: int,
    per_page: int,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    params = {"opportunity": "jobs", "page": page, "per_page": per_page}
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
    org = item.get("organisation") or item.get("organization") or {}
    job_detail = item.get("jobDetail") or item.get("job_detail") or {}

    loc_dicts = item.get("locations") or []
    loc_cities = []
    for ld in loc_dicts:
        if isinstance(ld, dict) and ld.get("city"):
            loc_cities.append(ld["city"])

    required_skills = item.get("required_skills") or item.get("skills") or []
    skill_names = []
    for s in required_skills:
        if isinstance(s, dict):
            skill_names.append(s.get("skill_name") or s.get("skill") or "")

    filters = item.get("filters") or []
    eligibility_tags = []
    for f in filters:
        if isinstance(f, dict) and f.get("name"):
            eligibility_tags.append(f["name"])

    salary = build_salary_numeric(job_detail)
    work_mode = build_work_mode(job_detail, region=item.get("region"))

    details_html = item.get("details") or ""
    details_text = _strip_tags(details_html) if details_html else ""

    return {
        "id": item.get("id"),
        "title": item.get("title"),
        "subtype": item.get("subtype"),
        "company_id": org.get("id") or item.get("organization_id"),
        "company": org.get("name"),
        "updated_at": item.get("updated_at"),
        "end_date": item.get("end_date"),
        "region": item.get("region"),
        "location_cities": _safe_join(loc_cities),
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


COLUMN_MAP = {
    "id": "job_id",
    "title": "job_title",
    "company": "company_name",
    "company_id": "company_id",
    "subtype": "oppurtunity_type",
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
    "eligibility": "eleigibility_criteria",
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
    "platform",
    "oppurtunity_type",
    "status",
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
    "eleigibility_criteria",
    "viewsCount",
    "registerCount",
    "application_url",
    "short_url",
    "public_url",
    "short_id",
    "logo_url",
    "details",
    "order",
]


def remap_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for src_key, dst_key in COLUMN_MAP.items():
        if src_key in row:
            out[dst_key] = row.get(src_key)

    out["platform"] = "unstop"
    out["status"] = 1
    out["order"] = 0

    jl = (out.get("job_location") or "").strip()
    if not jl:
        out["job_location"] = "Online"

    out["application_deadline"] = format_ist_date(out.get("application_deadline"))

    for k in OUTPUT_COLUMNS:
        out.setdefault(k, "")
    return out


def _coerce_job_id(row: Dict[str, Any]) -> Optional[int]:
    jid = row.get("job_id")
    if jid is None or str(jid).strip() == "":
        return None
    try:
        return int(jid)
    except Exception:
        return None


def _norm_opp_type(row: Dict[str, Any]) -> str:
    return str(row.get("oppurtunity_type") or "").strip().lower()


def _make_job_key(platform: str, opp_type: str, job_id: int) -> str:
    p = (platform or "").strip().lower()
    t = (opp_type or "").strip().lower()
    if not p:
        p = "unknown_platform"
    if not t:
        t = "unknown_type"
    return f"{p}:{t}:{job_id}"


def scrape_jobs(
    cfg: ScrapeConfig,
    *,
    existing_job_keys: Optional[Set[str]] = None,
    stop_when_page_all_seen: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    existing_job_keys = existing_job_keys or set()

    session, timeout_s = make_session()
    delta_rows: List[Dict[str, Any]] = []
    page = 1

    stats = {
        "pages_fetched": 0,
        "items_seen": 0,
        "kept_delta": 0,
        "skipped_existing": 0,
        "skipped_missing_job_id": 0,
        "early_stop_all_seen_page": 0,
    }

    while True:
        if cfg.max_pages and page > cfg.max_pages:
            break

        payload = fetch_page(session, timeout_s, page, cfg.per_page, cfg.extra_params)
        stats["pages_fetched"] += 1

        data_block = payload.get("data") or {}
        items = data_block.get("data") or []
        if not items:
            break

        page_all_seen = True

        for item in items:
            if not isinstance(item, dict):
                continue

            extracted = extract_row(item)
            mapped = remap_row(extracted)
            stats["items_seen"] += 1

            jid = _coerce_job_id(mapped)
            if jid is None:
                stats["skipped_missing_job_id"] += 1
                page_all_seen = False
                continue

            opp_type = _norm_opp_type(mapped)
            key = _make_job_key("unstop", opp_type, jid)

            if key in existing_job_keys:
                stats["skipped_existing"] += 1
                continue

            page_all_seen = False
            delta_rows.append(mapped)
            stats["kept_delta"] += 1

        if stop_when_page_all_seen and page_all_seen:
            stats["early_stop_all_seen_page"] = 1
            break

        page += 1

    return delta_rows, stats


def unstop(
    per_page: int = 18,
    max_pages: int = 0,
    *,
    existing_job_keys: Optional[Set[str]] = None,
    stop_when_page_all_seen: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    cfg = ScrapeConfig(per_page=int(per_page), max_pages=int(max_pages), extra_params=None)
    return scrape_jobs(cfg, existing_job_keys=existing_job_keys, stop_when_page_all_seen=stop_when_page_all_seen)


__all__ = ["unstop", "OUTPUT_COLUMNS"]
