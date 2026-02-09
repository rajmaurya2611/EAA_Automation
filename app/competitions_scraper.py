#!/usr/bin/env python3
"""
competition_scraper.py — Unstop Competitions Scraper (public API)

Fetches competitions from:
  https://unstop.com/api/public/opportunity/search-result?opportunity=competitions&page=1&per_page=18

Writes:
  - CSV
  - JSON (single file, NOT JSONL)

Keeps logic the same, but outputs ONLY the renamed fields requested:
- competition_id
- competition_title
- organization_name
- application_url
- application_deadline
- competition_location
- prizes
- competition_mode
- competition_type
- required_skills
- eligibility_criteria
- competition_logo_url
- competition_description
+ constants:
- competition_status = 1
- display_order = 0
- platform = "unstop"

No artificial delays; only backoff on errors/rate-limits via urllib3 Retry.
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
# Small helpers
# -----------------------------
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
    """
    prizes sometimes contain:
      currency="fa-rupee" or currencyCode="INR"
    We normalize to a short code string if present.
    """
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
    """
    Returns:
      - prizes_text: "Winner:15000 INR; First Runner Up:10000 INR; ..."
      - prize_min_cash
      - prize_max_cash

    Only uses numeric cash values (no symbols).
    """
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
            # no cash
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
    """
    Returns:
      - category_filters: comma-joined filter names where type == "category"
      - eligible_filters: comma-joined filter names where type == "eligible"
    """
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


def extract_location_from_address(addr: dict | None) -> Tuple[str, str, str, str]:
    """
    Competitions often have address_with_country_logo object (for offline events).
    Returns: address, city, state, country
    """
    a = addr or {}
    address = (a.get("address") or "").strip()

    city = (a.get("city") or "").strip()
    state = (a.get("state") or "").strip()

    country = ""
    c = a.get("country")
    if isinstance(c, dict):
        country = (c.get("name") or "").strip()
    elif isinstance(c, str):
        country = c.strip()

    return address, city, state, country


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
    out_prefix: str = "unstop_competitions"
    extra_params: Optional[Dict[str, Any]] = None


def fetch_page(
    session: requests.Session,
    timeout_s: int,
    page: int,
    per_page: int,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    params = {
        "opportunity": "competitions",
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
    org = item.get("organisation") or item.get("organization") or {}
    filters = item.get("filters") or []
    required_skills = item.get("required_skills") or item.get("skills") or []
    prizes = item.get("prizes") or []
    addr = item.get("address_with_country_logo") or {}

    _category_filters, eligible_filters = extract_filters(filters)
    skills = extract_skills(required_skills)
    _prizes_text, _prize_min_cash, prize_max_cash = summarize_prizes(prizes)

    address, _city, _state, _country = extract_location_from_address(addr)

    # If address blank => online
    if not address:
        address = "online"

    # Only keep prize_max_cash formatted
    if prize_max_cash is not None:
        prizes_out = f"Prizes upto {prize_max_cash}"
    else:
        prizes_out = ""

    details_html = item.get("details") or ""
    details_text = _strip_tags(details_html)

    return {
        "competition_id": item.get("id"),
        "competition_title": item.get("title"),
        "organization_name": org.get("name"),
        "application_url": item.get("seo_url"),
        "application_deadline": item.get("end_date"),
        "competition_location": address,
        "prizes": prizes_out,
        "competition_mode": item.get("region"),
        "competition_type": item.get("type"),
        "required_skills": skills,
        "eligibility_criteria": eligible_filters,
        "competition_logo_url": item.get("logoUrl2") or org.get("logoUrl2") or org.get("logoUrl"),
        "competition_description": details_text,
        "competition_status": 1,
        "display_order": 0,
        "platform": "unstop",
    }


def scrape_competitions(cfg: ScrapeConfig) -> List[Dict[str, Any]]:
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
                rows.append(extract_row(item))

        page += 1

    return rows


# -----------------------------
# Writers
# -----------------------------
def _timestamp_str() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def write_json(rows: List[Dict[str, Any]], path: str) -> None:
    # Single JSON file (array)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def write_csv(rows: List[Dict[str, Any]], path: str) -> None:
    if not rows:
        with open(path, "w", encoding="utf-8") as f:
            f.write("")
        return

    fieldnames = [
        "competition_id",
        "competition_title",
        "organization_name",
        "application_url",
        "application_deadline",
        "competition_location",
        "competition_mode",
        "competition_type",
        "required_skills",
        "eligibility_criteria",
        "prizes",
        "competition_logo_url",
        "competition_description",
        "competition_status",
        "display_order",
        "platform",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


# -----------------------------
# CLI
# -----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scrape Unstop competitions to CSV + JSON (renamed fields).")
    p.add_argument("--per-page", type=int, default=18)
    p.add_argument("--max-pages", type=int, default=0, help="0 = until empty")
    p.add_argument("--out-prefix", type=str, default="unstop_competitions")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    cfg = ScrapeConfig(
        per_page=max(1, int(args.per_page)),
        max_pages=max(0, int(args.max_pages)),
        out_prefix=str(args.out_prefix).strip() or "unstop_competitions",
        extra_params=None,  # no filters
    )

    rows = scrape_competitions(cfg)

    ts = _timestamp_str()
    csv_path = f"{cfg.out_prefix}_{ts}.csv"
    json_path = f"{cfg.out_prefix}_{ts}.json"

    write_csv(rows, csv_path)
    write_json(rows, json_path)

    print(f"[OK] rows={len(rows)}")
    print(f"[OK] wrote CSV  → {csv_path}")
    print(f"[OK] wrote JSON → {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
