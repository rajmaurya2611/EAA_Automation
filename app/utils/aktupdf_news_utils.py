import hashlib
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


def sanitize_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\\\|?*\x00-\x1F]', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:180] if name else "document"


def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def url_to_hash(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def normalize_pdf_url(href: str, base_url: str) -> str:
    return urljoin(base_url, href)


def extract_pdf_links_from_circular_page(html: str, base_url: str) -> list[dict]:
    """
    Extract circular rows from AKTU ERP circular page.
    Tries multiple selectors because markup may vary.
    """
    soup = BeautifulSoup(html, "html.parser")
    pdfs = []
    seen = set()

    # First pass: anchors that look like "देखें" / "view" or any anchor in table rows
    rows = soup.find_all("tr")

    for row in rows:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue

        anchors = row.find_all("a", href=True)
        if not anchors:
            continue

        for a in anchors:
            href = a.get("href", "").strip()
            if not href:
                continue

            full_url = normalize_pdf_url(href, base_url)

            # We allow non-.pdf hrefs too because some govt pages route downloads through handlers
            if full_url in seen:
                continue

            link_text = a.get_text(" ", strip=True).lower()

            # Build a title from row cells, preferring descriptive cells
            row_texts = [
                c.get_text(" ", strip=True)
                for c in cells
                if c.get_text(" ", strip=True)
            ]
            title = " | ".join(row_texts[:4]) if row_texts else a.get_text(" ", strip=True)

            # Heuristic: accept links that are in row and look like document/open/view links
            if (
                ".pdf" in full_url.lower()
                or "view" in link_text
                or "देखें" in link_text
                or "download" in link_text
                or "circular" in full_url.lower()
                or "attachment" in full_url.lower()
            ):
                seen.add(full_url)
                pdfs.append({
                    "title": title or Path(full_url).name,
                    "url": full_url,
                })

    # Fallback: any anchor with href containing pdf
    if not pdfs:
        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if not href:
                continue

            full_url = normalize_pdf_url(href, base_url)
            if ".pdf" not in full_url.lower():
                continue
            if full_url in seen:
                continue

            seen.add(full_url)
            pdfs.append({
                "title": a.get_text(" ", strip=True) or Path(full_url).name,
                "url": full_url,
            })

    return pdfs


def download_file(url: str, output_path: Path):
    with requests.get(url, headers=HEADERS, stream=True, timeout=90) as response:
        response.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)