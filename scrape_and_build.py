#!/usr/bin/env python3
"""
FlyttSignal — scraper + HTML-byggare
Kör manuellt eller via GitHub Actions cron.

Krav: pip install requests beautifulsoup4
"""

import json, re, time, datetime, sys, os
from pathlib import Path

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Installera: pip install requests beautifulsoup4")
    sys.exit(1)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FlyttSignal/1.0)",
    "Accept-Language": "sv-SE,sv;q=0.9",
}
TODAY = datetime.date.today().isoformat()
OUT   = Path(__file__).parent / "index.html"
SNAP  = Path(__file__).parent / "snapshot_prev.json"
TMPL  = Path(__file__).parent / "flyttsignal_v5_template.html"


# ── CASTELLUM ─────────────────────────────────────────────────────────────────

def castellum_get_links(city="STOCKHOLM"):
    links = set()
    for page in range(1, 8):
        url = f"https://www.castellum.se/fastigheter/Search/?City={city}&Text=&CurrentPage={page}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            before = len(links)
            for a in soup.find_all("a", href=True):
                h = a["href"]
                if re.match(r"/fastigheter/[a-z0-9-]+/?$", h):
                    links.add("https://www.castellum.se" + h)
            print(f"  Castellum sida {page}: {len(links)} unika ({len(links)-before} nya)")
            if len(links) == before:
                break
            time.sleep(0.3)
        except Exception as e:
            print(f"  Fel sida {page}: {e}")
            break
    return list(links)

def castellum_scrape_property(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        # Namn
        h1 = soup.find("h1")
        name = h1.get_text(strip=True) if h1 else ""

        # Adress via JSON-LD
        address = ""
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                d = json.loads(script.string)
                if d.get("@type") == "Place":
                    address = d.get("address", {}).get("streetAddress", "")
                    break
            except Exception:
                pass

        # Hyresgäster — leta efter exakt textnod "Hyresgäster i fastigheten"
        tenants = []
        for el in soup.find_all(string="Hyresgäster i fastigheten"):
            parent = el.parent.parent
            raw = parent.get_text(" ").replace("Hyresgäster i fastigheten", "").strip()
            tenants = [t.strip() for t in raw.split(" - ") if len(t.strip()) > 2]
            # Filtrera bort om tenant-strängen matchar fastighetens slug (dataläckage)
            slug = url.rstrip("/").split("/")[-1]
            tenants = [t for t in tenants if t.lower() != slug.replace("-", " ")]
            break

        if not tenants:
            return None

        slug = url.rstrip("/").split("/")[-1]
        return {
            "slug":    slug,
            "name":    name,
            "address": address,
            "url":     url,
            "source":  "Castellum",
            "tenants": tenants,
        }
    except Exception as e:
        print(f"    Fel: {e}")
        return None


# ── VASAKRONAN ────────────────────────────────────────────────────────────────
# OBS: Vasakronan listar INTE hyresgäster öppet per fastighet på samma sätt som
# Castellum. Hyresgästinfo-sidorna är inloggningsskyddade eller innehåller ej
# bolagsnamn i källkod. Verifieras manuellt nästa session.
# Platshållare implementerad — returnerar tom lista tills struktur bekräftats.

def vasakronan_get_links():
    # TODO: Bekräfta URL-mönster och att hyresgäster är publik info
    # Känd struktur: vasakronan.se/fastigheter/{slug}-{id}/
    # Fastighetslista finns som nedladdningsbar PDF — alternativ datakälla
    print("  Vasakronan: ej implementerad ännu — verifieras nästa session")
    return []

def vasakronan_scrape_property(url):
    return None


# ── FABEGE ────────────────────────────────────────────────────────────────────
# Fabege listar hyresgäster under /fastigheter/{slug}/ — men ej som strukturerad
# lista. Primär signal-källa: pressreleaser om nya hyresavtal.
# Verifieras manuellt nästa session.

def fabege_get_links():
    # TODO: Bekräfta att hyresgäster listas i HTML på fastighetssidor
    # Känd struktur: fabege.se/fastigheter/{slug}/
    print("  Fabege: ej implementerad ännu — verifieras nästa session")
    return []

def fabege_scrape_property(url):
    return None


# ── MAIN ──────────────────────────────────────────────────────────────────────

def scrape_all():
    all_data = []

    print("== Castellum ==")
    links = castellum_get_links()
    print(f"  {len(links)} fastighetslänkar hittade")
    for i, url in enumerate(links):
        result = castellum_scrape_property(url)
        if result:
            all_data.append(result)
        if i % 20 == 0:
            print(f"  {i+1}/{len(links)} klar...")
        time.sleep(0.1)
    print(f"  {sum(1 for d in all_data if d['source']=='Castellum')} fastigheter med hyresgäster")

    print("== Vasakronan ==")
    for url in vasakronan_get_links():
        result = vasakronan_scrape_property(url)
        if result:
            all_data.append(result)

    print("== Fabege ==")
    for url in fabege_get_links():
        result = fabege_scrape_property(url)
        if result:
            all_data.append(result)

    return all_data


def compute_delta(current, prev_snap):
    delta = {}
    prev = prev_snap.get("data", {})
    for f in current:
        old = prev.get(f["slug"])
        if not old:
            continue
        gone  = [t for t in old if t not in f["tenants"]]
        added = [t for t in f["tenants"] if t not in old]
        if gone or added:
            delta[f["slug"]] = {"gone": gone, "added": added, "snapDate": prev_snap.get("date", "?")}
    return delta


def build_html(data, delta):
    template = TMPL.read_text(encoding="utf-8")
    data_json = json.dumps(data, ensure_ascii=False)
    html = template.replace("%%DATA%%", data_json).replace("%%BUILD_DATE%%", TODAY)

    # Inject delta as initial DELTA state if we have one
    if delta:
        delta_js = "var _INITIAL_DELTA = " + json.dumps(delta, ensure_ascii=False) + ";\n"
        html = html.replace("var DELTA      = {};", "var DELTA      = {};\n" + delta_js +
                            "  // Pre-load delta from build\n  setTimeout(function(){ DELTA = _INITIAL_DELTA; updateDeltaPanel(); applyFilters(); }, 100);")

    OUT.write_text(html, encoding="utf-8")
    print(f"HTML skriven: {OUT} ({len(html):,} tecken)")


def save_snapshot(data):
    snap = {
        "date": TODAY,
        "data": {f["slug"]: f["tenants"] for f in data}
    }
    SNAP.write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Snapshot sparad: {SNAP}")


def main():
    print(f"FlyttSignal scraper — {TODAY}")
    print("-" * 40)

    # Ladda föregående snapshot om den finns
    prev_snap = {}
    if SNAP.exists():
        prev_snap = json.loads(SNAP.read_text(encoding="utf-8"))
        print(f"Föregående snapshot: {prev_snap.get('date', '?')} ({len(prev_snap.get('data', {}))} fastigheter)")

    # Scrapa
    data = scrape_all()
    total_tenants = sum(len(f["tenants"]) for f in data)
    print(f"\nTotalt: {len(data)} fastigheter, {total_tenants} hyresgäster")

    # Delta
    delta = {}
    if prev_snap:
        delta = compute_delta(data, prev_snap)
        if delta:
            print(f"\nDelta-signaler: {len(delta)} fastigheter med ändringar")
            for slug, d in list(delta.items())[:5]:
                f = next((x for x in data if x["slug"] == slug), None)
                name = f["name"] if f else slug
                print(f"  {name}: {len(d['gone'])} borta, {len(d['added'])} nya")
        else:
            print("\nDelta: inga ändringar sedan föregående snapshot")

    # Bygg HTML
    build_html(data, delta)

    # Spara ny snapshot (ersätter föregående)
    save_snapshot(data)

    print("\nKlar!")


if __name__ == "__main__":
    main()
