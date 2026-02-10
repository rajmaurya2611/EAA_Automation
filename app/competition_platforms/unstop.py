# app/job_platforms/unstop_competitions.py
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


def _currency_code(p: dict) -> str:
    code = (p.get("currencyCode") or "").strip()
    if code:
        return code
    cur = (p.get("currency") or "").strip().lower()
    if cur in {"fa-rupee", "inr", "rupee"}:
        return "INR"
    if cur:
        return cur.upper()
    return ""


def summarize_prizes(prizes: list | None) -> Tuple[str, Optional[int], Optional[int]]:
    if not prizes:
        return "", None, None

    parts: List[str] = []
    cash_vals: List[int] = []

    for p in prizes:
        if not isinstance(p, dict):
            continue
        rank = (p.get("rank") or "").strip()
        cash = _to_int(p.get("cash"))
        code = _currency_code(p)
        other = (p.get("others") or "").strip()

        if cash is not None:
            cash_vals.append(cash)
            if rank:
                parts.append(f"{rank}:{cash} {code}".strip())
            else:
                parts.append(f"{cash} {code}".strip())
        else:
            if rank and other:
                parts.append(f"{rank}:{other}")
            elif rank:
                parts.append(f"{rank}")
            elif other:
                parts.append(other)

    prizes_text = "; ".join([p for p in parts if p])
    if cash_vals:
        return prizes_text, min(cash_vals), max(cash_vals)
    return prizes_text, None, None


def extract_filters(filters: list | None) -> Tuple[str, str]:
    if not filters:
        return "", ""

    categories: List[str] = []
    eligible: List[str] = []

    for f in filters:
        if not isinstance(f, dict):
            continue
        name = (f.get("name") or "").strip()
        ftype = (f.get("type") or "").strip().lower()
        if not name:
            continue
        if ftype == "category":
            categories.append(name)
        elif ftype == "eligible":
            eligible.append(name)

    return _safe_join(categories), _safe_join(eligible)


def extract_skills(required_skills: list | None) -> str:
    if not required_skills:
        return ""
    out: List[str] = []
    for s in required_skills:
        if isinstance(s, dict):
            out.append((s.get("skill_name") or s.get("skill") or "").strip())
    return _safe_join([x for x in out if x])


def extract_location_from_address(addr: dict | None) -> str:
    a = addr or {}
    address = (a.get("address") or "").strip()
    return address


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
    params = {"opportunity": "competitions", "page": page, "per_page": per_page}
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


OUTPUT_COLUMNS = [
    "competition_id",
    "competition_title",
    "organization_name",
    "application_url",
    "application_deadline",
    "competition_location",
    "prizes",
    "competition_mode",
    "competition_type",
    "required_skills",
    "eligibility_criteria",
    "competition_logo_url",
    "competition_description",
    "competition_status",
    "display_order",
    "platform",
]


def _coerce_comp_id(v: Any) -> Optional[int]:
    if v is None or str(v).strip() == "":
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return None


def _make_comp_key(platform: str, comp_type: str, comp_id: int) -> str:
    p = (platform or "").strip().lower() or "unknown_platform"
    t = (comp_type or "").strip().lower() or "unknown_type"
    return f"{p}:{t}:{comp_id}"


def extract_row(item: Dict[str, Any]) -> Dict[str, Any]:
    org = item.get("organisation") or item.get("organization") or {}
    filters = item.get("filters") or []
    required_skills = item.get("required_skills") or item.get("skills") or []
    prizes = item.get("prizes") or []
    addr = item.get("address_with_country_logo") or {}

    _, eligible_filters = extract_filters(filters)
    skills = extract_skills(required_skills)
    _, _, prize_max_cash = summarize_prizes(prizes)

    address = extract_location_from_address(addr)
    if not address:
        address = "online"

    prizes_out = f"Prizes upto {prize_max_cash}" if prize_max_cash is not None else ""

    details_html = item.get("details") or ""
    details_text = _strip_tags(details_html)

    comp_type = str(item.get("type") or "").strip()

    return {
        "competition_id": item.get("id"),
        "competition_title": item.get("title"),
        "organization_name": org.get("name"),
        "application_url": item.get("seo_url"),
        "application_deadline": format_ist_date(item.get("end_date")),
        "competition_location": address,
        "prizes": prizes_out,
        "competition_mode": item.get("region"),
        "competition_type": comp_type,
        "required_skills": skills,
        "eligibility_criteria": eligible_filters,
        "competition_logo_url": item.get("logoUrl2") or org.get("logoUrl2") or org.get("logoUrl"),
        "competition_description": details_text,
        "competition_status": 1,
        "display_order": 0,
        "platform": "unstop",
    }


def scrape_competitions(
    cfg: ScrapeConfig,
    *,
    existing_comp_keys: Optional[Set[str]] = None,
    stop_when_page_all_seen: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    existing_comp_keys = existing_comp_keys or set()

    session, timeout_s = make_session()
    delta_rows: List[Dict[str, Any]] = []
    page = 1

    stats = {
        "pages_fetched": 0,
        "items_seen": 0,
        "kept_delta": 0,
        "skipped_existing": 0,
        "skipped_missing_competition_id": 0,
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

            row = extract_row(item)
            stats["items_seen"] += 1

            cid = _coerce_comp_id(row.get("competition_id"))
            if cid is None:
                stats["skipped_missing_competition_id"] += 1
                page_all_seen = False
                continue

            comp_type = str(row.get("competition_type") or "").strip()
            key = _make_comp_key("unstop", comp_type, cid)

            if key in existing_comp_keys:
                stats["skipped_existing"] += 1
                continue

            page_all_seen = False
            delta_rows.append(row)
            stats["kept_delta"] += 1

        if stop_when_page_all_seen and page_all_seen:
            stats["early_stop_all_seen_page"] = 1
            break

        page += 1

    return delta_rows, stats


def unstop_competitions(
    per_page: int = 18,
    max_pages: int = 0,
    *,
    existing_comp_keys: Optional[Set[str]] = None,
    stop_when_page_all_seen: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    cfg = ScrapeConfig(per_page=int(per_page), max_pages=int(max_pages), extra_params=None)
    return scrape_competitions(cfg, existing_comp_keys=existing_comp_keys, stop_when_page_all_seen=stop_when_page_all_seen)


__all__ = ["unstop_competitions", "OUTPUT_COLUMNS"]
