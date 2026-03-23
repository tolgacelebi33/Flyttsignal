#!/usr/bin/env python3
"""
FlyttSignal — Pressrelease-scraper
Hämtar uthyrningsmeddelanden från Castellum, Vasakronan och Fabege.
Returnerar strukturerade flytt-signaler med bolagsnamn, fastighet, kvm och datum.

Krav: pip install requests beautifulsoup4
"""

import re, json, time, datetime
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FlyttSignal/1.0)",
    "Accept-Language": "sv-SE,sv;q=0.9",
}

TODAY = datetime.date.today().isoformat()


# ── HJÄLPFUNKTIONER ───────────────────────────────────────────────────────────

def get(url, timeout=15):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  GET-fel {url}: {e}")
        return None


def extract_companies(text):
    """
    Extrahera bolagsnamn ur pressrelease-text.
    Letar efter mönster som "avtal med X AB", "hyresavtal med X", "välkomnar X".
    Returnerar lista med bolagsnamn.
    """
    patterns = [
        r"(?:avtal med|hyresavtal med|välkomnar|tecknat med|förhyrning till|hyr ut till)\s+([A-ZÅÄÖ][^\.,\n]{2,60?}(?:AB|Aktiebolag|HB|KB|GmbH|Inc|Ltd|AS|Oy))",
        r"([A-ZÅÄÖ][^\.,\n]{2,60?}(?:AB|Aktiebolag|HB|KB|GmbH|Inc|Ltd|AS|Oy))\s+(?:tecknar|hyr|flyttar|etablerar|tillträder)",
        r"([A-ZÅÄÖ][^\.,\n]{2,60?}(?:AB|Aktiebolag|HB|KB|GmbH|Inc|Ltd|AS|Oy))\s+(?:som|och)\s+(?:är|blir|har)",
    ]
    found = set()
    for p in patterns:
        for m in re.finditer(p, text, re.IGNORECASE):
            name = m.group(1).strip().rstrip(".,;:")
            if 3 < len(name) < 80:
                found.add(name)
    return sorted(found)


def extract_sqm(text):
    """Extrahera kvadratmetertal ur text."""
    m = re.search(r"(\d[\d\s]*)\s*(?:kvadratmeter|kvm|m²|m2)", text, re.IGNORECASE)
    if m:
        return int(re.sub(r"\s", "", m.group(1)))
    return None


def extract_date(text):
    """Extrahera tillträdesdatum ur text."""
    patterns = [
        r"tillträde(?:r|s|sdatum)?\s+(?:den\s+)?(\d{1,2}\s+\w+\s+\d{4})",
        r"inflyttning\s+(?:är\s+)?planerad(?:\s+till)?\s+(\d{1,2}\s+\w+\s+\d{4}|\w+\s+\d{4}|\d{4}-\d{2}-\d{2})",
        r"tillträde[r]?\s+(\d{4}-\d{2}-\d{2})",
        r"tillträde[r]?\s+den\s+(\d{1,2}\s+\w+\s+\d{4})",
        r"(\d{4}-\d{2}-\d{2})",  # fallback: ISO-datum i text
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def extract_property_name(text, source):
    """Extrahera fastighetsnamn ur text."""
    patterns = [
        r"(?:fastigheten|i)\s+([A-ZÅÄÖ][a-zåäö]+(?:\s+\d+[A-Za-z]?)?)",  # "fastigheten Klara C"
        r"(?:på|i)\s+([A-ZÅÄÖ][a-zåäö]+(?:\s+\d+)?)\s+(?:i\s+)?(?:Stockholm|Solna|Danderyd|Nacka)",
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            return m.group(1).strip()
    return None


# ── CASTELLUM ─────────────────────────────────────────────────────────────────

def castellum_press_links(max_pages=5):
    """Hämta alla pressrelease-URL:er från Castellums media-sida."""
    links = []
    base = "https://www.castellum.se/media/"

    soup = get(base)
    if not soup:
        return links

    # Castellum har en listning med pressmeddelanden + artiklar
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if "/media/pressmeddelanden/pressmeddelande/" in h or "/media/artiklar/" in h:
            full = h if h.startswith("http") else "https://www.castellum.se" + h
            if full not in links:
                links.append(full)

    return links


def castellum_parse_press(url):
    """Parsa en enskild Castellum-pressrelease."""
    soup = get(url)
    if not soup:
        return None

    title = soup.find("h1")
    title_text = title.get_text(strip=True) if title else ""

    # Kräver att det handlar om uthyrning i Stockholm
    sthlm_keywords = ["stockholm", "solna", "danderyd", "kista", "hammarby", "nacka", "bromma"]
    lease_keywords = ["hyresavtal", "hyr ut", "förhyrning", "uthyrning", "tecknat avtal", "välkomnar"]

    body = soup.find("article") or soup.find("main") or soup.find("body")
    body_text = body.get_text(" ", strip=True) if body else ""
    full_text = (title_text + " " + body_text).lower()

    if not any(k in full_text for k in lease_keywords):
        return None
    if not any(k in full_text for k in sthlm_keywords):
        return None

    companies = extract_companies(title_text + " " + body_text)
    sqm = extract_sqm(body_text)
    move_date = extract_date(body_text)
    prop_name = extract_property_name(body_text, "Castellum")

    # Publiceringsdatum
    date_el = soup.find("time") or soup.find(class_=re.compile(r"date|time|publi", re.I))
    pub_date = date_el.get("datetime", date_el.get_text(strip=True))[:10] if date_el else TODAY

    return {
        "source":      "Castellum",
        "title":       title_text,
        "url":         url,
        "companies":   companies,
        "property":    prop_name,
        "sqm":         sqm,
        "move_date":   move_date,
        "pub_date":    pub_date,
        "confirmed":   True,
    }


# ── VASAKRONAN ────────────────────────────────────────────────────────────────

def vasakronan_press_links():
    """Hämta Vasakronans pressrelease-URL:er."""
    links = []
    soup = get("https://vasakronan.se/om-vasakronan/press/pressmeddelanden/")
    if not soup:
        return links

    for a in soup.find_all("a", href=True):
        h = a["href"]
        if "/pressmeddelande/" in h:
            full = h if h.startswith("http") else "https://vasakronan.se" + h
            if full not in links:
                links.append(full)

    return links


def vasakronan_parse_press(url):
    """Parsa en enskild Vasakronan-pressrelease."""
    soup = get(url)
    if not soup:
        return None

    title = soup.find("h1")
    title_text = title.get_text(strip=True) if title else ""

    sthlm_keywords = ["stockholm", "solna", "city", "kista", "hammarby", "östermalm", "norrmalm"]
    lease_keywords = ["hyresavtal", "hyr ut", "förhyrning", "tecknat avtal", "kvadratmeter", "kvm"]

    body = soup.find("article") or soup.find("main") or soup.body
    body_text = body.get_text(" ", strip=True) if body else ""
    full_text = (title_text + " " + body_text).lower()

    if not any(k in full_text for k in lease_keywords):
        return None
    if not any(k in full_text for k in sthlm_keywords):
        return None

    companies = extract_companies(title_text + " " + body_text)
    sqm = extract_sqm(body_text)
    move_date = extract_date(body_text)
    prop_name = extract_property_name(body_text, "Vasakronan")

    date_el = soup.find("time") or soup.find(class_=re.compile(r"date|meta|publi", re.I))
    pub_date = date_el.get("datetime", "")[:10] if date_el else TODAY
    if not pub_date:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", body_text)
        pub_date = m.group(1) if m else TODAY

    return {
        "source":      "Vasakronan",
        "title":       title_text,
        "url":         url,
        "companies":   companies,
        "property":    prop_name,
        "sqm":         sqm,
        "move_date":   move_date,
        "pub_date":    pub_date,
        "confirmed":   True,
    }


# ── FABEGE ────────────────────────────────────────────────────────────────────

def fabege_press_links():
    """Hämta Fabeges nyheter/pressrelease-URL:er."""
    links = []
    soup = get("https://www.fabege.se/om-fabege/pressrum/nyheter/")
    if not soup:
        return links

    for a in soup.find_all("a", href=True):
        h = a["href"]
        if "/pressrum/nyheter/" in h and h.count("/") > 4:  # filtrerar bort kategorisidan
            full = h if h.startswith("http") else "https://www.fabege.se" + h
            if full not in links:
                links.append(full)

    return links


def fabege_parse_press(url):
    """Parsa en enskild Fabege-pressrelease."""
    soup = get(url)
    if not soup:
        return None

    title = soup.find("h1")
    title_text = title.get_text(strip=True) if title else ""

    lease_keywords = ["hyresavtal", "hyr ut", "förhyrning", "tecknar avtal", "välkomnar", "kvadratmeter"]

    body = soup.find("article") or soup.find("main") or soup.body
    body_text = body.get_text(" ", strip=True) if body else ""
    full_text = (title_text + " " + body_text).lower()

    if not any(k in full_text for k in lease_keywords):
        return None

    companies = extract_companies(title_text + " " + body_text)
    sqm = extract_sqm(body_text)
    move_date = extract_date(body_text)
    prop_name = extract_property_name(body_text, "Fabege")

    date_el = soup.find("time")
    pub_date = date_el.get("datetime", "")[:10] if date_el else ""
    if not pub_date:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", body_text)
        pub_date = m.group(1) if m else TODAY

    return {
        "source":      "Fabege",
        "title":       title_text,
        "url":         url,
        "companies":   companies,
        "property":    prop_name,
        "sqm":         sqm,
        "move_date":   move_date,
        "pub_date":    pub_date,
        "confirmed":   True,
    }


# ── MAIN ──────────────────────────────────────────────────────────────────────

def scrape_all_press(since_date=None):
    """
    Hämta alla relevanta pressreleaser.
    since_date: str ISO-datum "2025-01-01" — filtrera bort äldre
    """
    results = []

    sources = [
        ("Castellum",   castellum_press_links,   castellum_parse_press),
        ("Vasakronan",  vasakronan_press_links,   vasakronan_parse_press),
        ("Fabege",      fabege_press_links,        fabege_parse_press),
    ]

    for name, get_links, parse in sources:
        print(f"\n== {name} press ==")
        links = get_links()
        print(f"  {len(links)} länkar hittade")

        for url in links:
            item = parse(url)
            if item:
                if since_date and item["pub_date"] < since_date:
                    continue
                results.append(item)
                print(f"  ✓ [{item['pub_date']}] {item['title'][:60]}")
                if item["companies"]:
                    print(f"    Bolag: {', '.join(item['companies'][:3])}")
            time.sleep(0.2)

    return results


def match_signals_to_tenants(press_signals, tenant_data):
    """
    Korsreferera pressreleaser mot hyresgästdatan.
    Returnerar dict: { bolagsnamn_lowercase -> [signal, ...] }
    
    Om ett bolag nämns i en pressrelease (ny hyresgäst någonstans)
    OCH finns i en fastighets hyresgästlista
    → det är en bekräftad flytt-signal för alla som levererar till det bolaget.
    
    Om ett bolag är borta ur en snapshot OCH nämns i en pressrelease
    → dubbelt bekräftad flytt.
    """
    # Index: bolagsnamn (lowercase) -> lista av signaler
    press_index = {}
    for sig in press_signals:
        for company in sig.get("companies", []):
            key = company.lower().strip()
            if key not in press_index:
                press_index[key] = []
            press_index[key].append(sig)

    # Matcha mot hyresgästdata
    confirmed_moves = {}
    for f in tenant_data:
        for tenant in f["tenants"]:
            tenant_lower = tenant.lower().strip()
            # Fuzzy-match: kolla om pressrelease-bolaget ingår i tenant-strängen
            for press_company, signals in press_index.items():
                # Enkel substring-match + minimumlängd
                if len(press_company) > 5 and (
                    press_company in tenant_lower or
                    tenant_lower in press_company
                ):
                    key = (f["slug"], tenant)
                    if key not in confirmed_moves:
                        confirmed_moves[key] = []
                    confirmed_moves[key].extend(signals)

    return confirmed_moves


if __name__ == "__main__":
    # Hämta pressreleaser från senaste 90 dagarna
    cutoff = (datetime.date.today() - datetime.timedelta(days=90)).isoformat()
    signals = scrape_all_press(since_date=cutoff)

    print(f"\n=== Totalt {len(signals)} relevanta pressreleaser ===")
    for s in signals[:10]:
        print(f"  [{s['source']}] {s['pub_date']} — {s['title'][:55]}")
        if s["companies"]:
            print(f"    → {', '.join(s['companies'][:4])}")

    # Spara till JSON för användning i build-scriptet
    out_file = "press_signals.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(signals, f, ensure_ascii=False, indent=2)
    print(f"\nSparat: {out_file}")
