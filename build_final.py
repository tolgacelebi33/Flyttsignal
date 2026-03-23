#!/usr/bin/env python3
"""
FlyttSignal — komplett build-skript
Kör: python build_final.py

1. Scraper hyresgäster (Castellum + platshållare för Vasakronan/Fabege)
2. Scraper pressreleaser från alla tre
3. Korsrefererar: bekräftade flytt-signaler om ett bolag
   - Försvunnit ur snapshot OCH nämns i ny pressrelease = BEKRÄFTAD
   - Enbart försvunnit ur snapshot = MÖJLIG
   - Enbart i pressrelease (ny på plats) = BEKRÄFTAD (inkommande)
4. Bäddar in data + signaler i index.html

Krav: pip install requests beautifulsoup4
"""

import json, re, time, datetime, sys
from pathlib import Path

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("Installera: pip install requests beautifulsoup4")

from scrape_press import scrape_all_press, match_signals_to_tenants

TODAY     = datetime.date.today().isoformat()
TMPL      = Path(__file__).parent / "flyttsignal_v5_template.html"
OUT       = Path(__file__).parent / "index.html"
SNAP_FILE = Path(__file__).parent / "snapshot_prev.json"
PRESS_FILE= Path(__file__).parent / "press_signals.json"
HEADERS   = {"User-Agent": "Mozilla/5.0 (compatible; FlyttSignal/1.0)",
              "Accept-Language": "sv-SE,sv;q=0.9"}


# ── CASTELLUM-SCRAPER (samma som tidigare) ────────────────────────────────────

def castellum_links(city="STOCKHOLM"):
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
            if len(links) == before:
                break
            time.sleep(0.25)
        except Exception as e:
            print(f"  Sida {page}: {e}")
            break
    return list(links)


def castellum_property(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        h1 = soup.find("h1")
        name = h1.get_text(strip=True) if h1 else ""
        address = ""
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                d = json.loads(s.string)
                if d.get("@type") == "Place":
                    address = d.get("address", {}).get("streetAddress", "")
                    break
            except Exception:
                pass
        tenants = []
        for node in soup.find_all(string="Hyresgäster i fastigheten"):
            raw = node.parent.parent.get_text(" ").replace("Hyresgäster i fastigheten", "").strip()
            slug = url.rstrip("/").split("/")[-1]
            slug_words = set(slug.replace("-", " ").lower().split())
            tenants = [
                t.strip() for t in raw.split(" - ")
                if len(t.strip()) > 2
                and not (set(t.lower().split()) & slug_words and len(t.split()) <= 3)
            ]
            break
        if not tenants:
            return None
        return {
            "slug":    url.rstrip("/").split("/")[-1],
            "name":    name,
            "address": address,
            "url":     url,
            "source":  "Castellum",
            "tenants": tenants,
        }
    except Exception as e:
        print(f"  Fel {url}: {e}")
        return None


def scrape_tenants():
    data = []
    print("== Castellum ==")
    links = castellum_links()
    print(f"  {len(links)} fastigheter")
    for i, url in enumerate(links):
        r = castellum_property(url)
        if r:
            data.append(r)
        if (i + 1) % 25 == 0:
            print(f"  {i+1}/{len(links)} klara ({len(data)} med hyresgäster)")
        time.sleep(0.1)
    print(f"  Totalt: {len(data)} fastigheter med hyresgäster")
    # Vasakronan + Fabege: platshållare tills structure verifierats
    return data


# ── DELTA ─────────────────────────────────────────────────────────────────────

def compute_delta(current, prev_snap):
    """Jämför mot föregående snapshot. Returnerar per-slug dict."""
    delta = {}
    prev = prev_snap.get("data", {})
    for f in current:
        old = prev.get(f["slug"])
        if not old:
            continue
        gone  = [t for t in old if t not in f["tenants"]]
        added = [t for t in f["tenants"] if t not in old]
        if gone or added:
            delta[f["slug"]] = {
                "gone":      gone,
                "added":     added,
                "snapDate":  prev_snap.get("date", "?"),
                "confirmed": [],   # fylls på av press-matchning
            }
    return delta


def enrich_with_press(delta, press_signals, tenant_data):
    """
    Korsreferera delta mot pressreleaser.
    Om ett borta-bolag nämns i en ny pressrelease → bekräftad flytt.
    """
    # Bygg press-index: bolagsnamn_lower -> [signal]
    press_index = {}
    for sig in press_signals:
        for company in sig.get("companies", []):
            key = company.lower().strip()
            press_index.setdefault(key, []).append(sig)

    enriched = 0
    for slug, d in delta.items():
        for tenant in d["gone"]:
            t_lower = tenant.lower().strip()
            for press_key, signals in press_index.items():
                if len(press_key) > 5 and (press_key in t_lower or t_lower in press_key):
                    # Matchar — lägg till bekräftelse
                    for sig in signals:
                        if sig not in d["confirmed"]:
                            d["confirmed"].append({
                                "company":  tenant,
                                "title":    sig["title"],
                                "url":      sig["url"],
                                "pub_date": sig["pub_date"],
                                "source":   sig["source"],
                                "property": sig.get("property"),
                                "sqm":      sig.get("sqm"),
                                "move_date":sig.get("move_date"),
                            })
                            enriched += 1

    if enriched:
        print(f"  Press-matchning: {enriched} bekräftade flytt-signaler")
    return delta


# ── HTML-BUILD ────────────────────────────────────────────────────────────────

def build_html(data, delta, press_signals):
    template = TMPL.read_text(encoding="utf-8")

    # Bygg CONFIRMED_MOVES: slug -> lista med bekräftade flytt-objekt
    confirmed_map = {}
    for slug, d in delta.items():
        if d.get("confirmed"):
            confirmed_map[slug] = d["confirmed"]

    data_json    = json.dumps(data, ensure_ascii=False)
    delta_json   = json.dumps(delta, ensure_ascii=False)
    confirm_json = json.dumps(confirmed_map, ensure_ascii=False)

    html = (template
        .replace("%%DATA%%",      data_json)
        .replace("%%BUILD_DATE%%", TODAY))

    # Injicera delta och confirmed_map som JS-variabler
    inject = (
        f"var _DELTA_INIT = {delta_json};\n"
        f"var _CONFIRMED  = {confirm_json};\n"
        "setTimeout(function(){\n"
        "  DELTA = _DELTA_INIT;\n"
        "  CONFIRMED = _CONFIRMED;\n"
        "  updateDeltaPanel();\n"
        "  applyFilters();\n"
        "}, 50);\n"
    )
    html = html.replace(
        "var DELTA      = {};",
        "var DELTA      = {};\nvar CONFIRMED  = {};\n" + inject
    )

    OUT.write_text(html, encoding="utf-8")
    print(f"\nHTML: {OUT}  ({len(html):,} tecken)")


def save_snapshot(data):
    snap = {"date": TODAY, "data": {f["slug"]: f["tenants"] for f in data}}
    SNAP_FILE.write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Snapshot: {SNAP_FILE}")


def main():
    print(f"FlyttSignal build — {TODAY}")
    print("=" * 45)

    # 1. Hyresgäster
    data = scrape_tenants()
    total_t = sum(len(f["tenants"]) for f in data)
    print(f"\nTotalt: {len(data)} fastigheter, {total_t} hyresgäster")

    # 2. Delta
    delta = {}
    if SNAP_FILE.exists():
        prev = json.loads(SNAP_FILE.read_text(encoding="utf-8"))
        print(f"\nFöregående snapshot: {prev.get('date')} ({len(prev.get('data',{}))} fastigheter)")
        delta = compute_delta(data, prev)
        n_gone  = sum(len(d["gone"])  for d in delta.values())
        n_added = sum(len(d["added"]) for d in delta.values())
        print(f"Delta: {len(delta)} fastigheter med ändringar ({n_gone} borta, {n_added} nya)")

    # 3. Pressreleaser (senaste 90 dagarna)
    cutoff = (datetime.date.today() - datetime.timedelta(days=90)).isoformat()
    print("\n== Pressreleaser ==")
    press = scrape_all_press(since_date=cutoff)
    print(f"  {len(press)} relevanta pressreleaser")
    PRESS_FILE.write_text(json.dumps(press, ensure_ascii=False, indent=2), encoding="utf-8")

    # 4. Berika delta med press-bekräftelse
    if delta and press:
        print("\n== Press-matchning ==")
        delta = enrich_with_press(delta, press, data)

    # 5. Bygg HTML
    build_html(data, delta, press)

    # 6. Spara ny snapshot
    save_snapshot(data)

    print("\nKlar!")


if __name__ == "__main__":
    main()
