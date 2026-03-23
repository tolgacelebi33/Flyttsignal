#!/usr/bin/env python3
"""
FlyttSignal — build script
Kör: python build_final_v2.py
Krav: pip install requests beautifulsoup4
"""

import json, re, time, datetime, sys
from pathlib import Path

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("Kör: pip install requests beautifulsoup4")

TODAY     = datetime.date.today().isoformat()
HERE      = Path(__file__).parent
TMPL      = HERE / "flyttsignal_v5_template.html"
OUT       = HERE / "index.html"
SNAP_FILE = HERE / "snapshot_prev.json"
HEADERS   = {
    "User-Agent": "Mozilla/5.0 (compatible; FlyttSignal/1.0)",
    "Accept-Language": "sv-SE,sv;q=0.9",
}


# ── SCRAPE CASTELLUM ──────────────────────────────────────────────────────────

def get_links():
    links = set()
    for page in range(1, 8):
        url = f"https://www.castellum.se/fastigheter/Search/?City=STOCKHOLM&Text=&CurrentPage={page}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            before = len(links)
            for a in soup.find_all("a", href=True):
                h = a["href"]
                if re.match(r"/fastigheter/[a-z0-9-]+/?$", h):
                    links.add("https://www.castellum.se" + h)
            print(f"  Sida {page}: {len(links)} fastigheter")
            if len(links) == before:
                break
            time.sleep(0.3)
        except Exception as e:
            print(f"  Sida {page} fel: {e}")
            break
    return list(links)


def scrape_property(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        h1 = soup.find("h1")
        name = h1.get_text(strip=True) if h1 else ""
        address = ""
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                d = json.loads(s.string or "")
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


# ── SCRAPE PRESSRELEASER ──────────────────────────────────────────────────────

def scrape_press():
    """Hämta uthyrningsmeddelanden från Castellum, Vasakronan och Fabege."""
    results = []

    sources = [
        ("Castellum",  "https://www.castellum.se/media/",
         "/media/pressmeddelanden/pressmeddelande/", "/media/artiklar/"),
        ("Vasakronan", "https://vasakronan.se/om-vasakronan/press/pressmeddelanden/",
         "/pressmeddelande/", None),
        ("Fabege",     "https://www.fabege.se/om-fabege/pressrum/nyheter/",
         "/pressrum/nyheter/", None),
    ]

    lease_kw  = ["hyresavtal", "hyr ut", "förhyrning", "tecknat avtal", "kvadratmeter", "kvm", "välkomnar"]
    sthlm_kw  = ["stockholm", "solna", "kista", "danderyd", "hammarby", "nacka", "bromma", "city"]

    for source_name, index_url, *path_patterns in sources:
        try:
            r = requests.get(index_url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            links = set()
            for a in soup.find_all("a", href=True):
                h = a["href"]
                for pat in path_patterns:
                    if pat and pat in h and h.count("/") > 3:
                        full = h if h.startswith("http") else f"https://{index_url.split('/')[2]}{h}"
                        links.add(full)
            print(f"  {source_name}: {len(links)} pressreleaser")

            for link in list(links)[:30]:  # max 30 per källa
                try:
                    pr = requests.get(link, headers=HEADERS, timeout=15)
                    psoup = BeautifulSoup(pr.text, "html.parser")
                    title_el = psoup.find("h1")
                    title = title_el.get_text(strip=True) if title_el else ""
                    body_el = psoup.find("article") or psoup.find("main") or psoup.body
                    body = body_el.get_text(" ", strip=True) if body_el else ""
                    full_text = (title + " " + body).lower()

                    if not any(k in full_text for k in lease_kw):
                        continue
                    if source_name != "Fabege" and not any(k in full_text for k in sthlm_kw):
                        continue

                    # Extrahera bolagsnamn
                    companies = []
                    for pat in [
                        r"(?:avtal med|hyresavtal med|välkomnar|tecknar med)\s+([A-ZÅÄÖ][^\.,\n]{2,60?}(?:AB|HB|KB|Inc|Ltd|AS))",
                        r"([A-ZÅÄÖ][^\.,\n]{2,60?}(?:AB|HB|KB|Inc|Ltd|AS))\s+(?:tecknar|hyr|flyttar|etablerar)",
                    ]:
                        for m in re.finditer(pat, title + " " + body):
                            name = m.group(1).strip().rstrip(".,;:")
                            if 3 < len(name) < 80:
                                companies.append(name)

                    sqm_m = re.search(r"(\d[\d\s]*)\s*(?:kvadratmeter|kvm)", body, re.I)
                    sqm = int(re.sub(r"\s", "", sqm_m.group(1))) if sqm_m else None

                    date_el = psoup.find("time")
                    pub_date = (date_el.get("datetime", "")[:10] if date_el else "") or TODAY

                    if companies:
                        results.append({
                            "source":    source_name,
                            "title":     title,
                            "url":       link,
                            "companies": list(set(companies)),
                            "sqm":       sqm,
                            "pub_date":  pub_date,
                        })
                    time.sleep(0.15)
                except Exception:
                    pass
        except Exception as e:
            print(f"  {source_name} index fel: {e}")

    return results


# ── DELTA ─────────────────────────────────────────────────────────────────────

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
            delta[f["slug"]] = {
                "gone":      gone,
                "added":     added,
                "snapDate":  prev_snap.get("date", "?"),
                "confirmed": [],
            }
    return delta


def enrich_delta(delta, press):
    press_index = {}
    for sig in press:
        for company in sig.get("companies", []):
            press_index.setdefault(company.lower().strip(), []).append(sig)

    for slug, d in delta.items():
        for tenant in d["gone"]:
            t_lower = tenant.lower().strip()
            for press_key, signals in press_index.items():
                if len(press_key) > 5 and (press_key in t_lower or t_lower in press_key):
                    for sig in signals:
                        d["confirmed"].append({
                            "company":  tenant,
                            "title":    sig["title"],
                            "url":      sig["url"],
                            "pub_date": sig["pub_date"],
                            "source":   sig["source"],
                            "sqm":      sig.get("sqm"),
                        })
    return delta


# ── BYGG HTML ─────────────────────────────────────────────────────────────────

def build_html(data, delta):
    if not TMPL.exists():
        sys.exit(f"Template saknas: {TMPL}")

    tmpl = TMPL.read_text(encoding="utf-8")

    confirmed_map = {slug: d["confirmed"] for slug, d in delta.items() if d.get("confirmed")}

    data_json    = json.dumps(data,          ensure_ascii=False)
    delta_json   = json.dumps(delta,         ensure_ascii=False)
    confirm_json = json.dumps(confirmed_map, ensure_ascii=False)

    html = tmpl.replace("%%DATA%%", data_json).replace("%%BUILD_DATE%%", TODAY)

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
        "var DELTA      = {};\nvar CONFIRMED  = {};\n" + inject,
        1
    )

    OUT.write_text(html, encoding="utf-8")
    print(f"\nindex.html: {len(html):,} tecken")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"FlyttSignal — {TODAY}")
    print("=" * 40)

    # 1. Scrapa hyresgäster
    print("\n== Castellum ==")
    links = get_links()
    print(f"  {len(links)} fastigheter hittade")
    data = []
    for i, url in enumerate(links):
        result = scrape_property(url)
        if result:
            data.append(result)
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(links)} klara...")
        time.sleep(0.1)

    total_t = sum(len(f["tenants"]) for f in data)
    print(f"  Resultat: {len(data)} fastigheter, {total_t} hyresgäster")

    if not data:
        print("VARNING: Ingen data hämtad -- avbryter")
        sys.exit(1)

    # 2. Delta
    delta = {}
    if SNAP_FILE.exists():
        prev = json.loads(SNAP_FILE.read_text(encoding="utf-8"))
        print(f"\nFöregående snapshot: {prev.get('date')} ({len(prev.get('data',{}))} fastigheter)")
        delta = compute_delta(data, prev)
        n_gone  = sum(len(d["gone"])  for d in delta.values())
        n_added = sum(len(d["added"]) for d in delta.values())
        print(f"Delta: {len(delta)} fastigheter ({n_gone} borta, {n_added} nya)")

    # 3. Pressreleaser
    print("\n== Pressreleaser ==")
    press = scrape_press()
    print(f"  {len(press)} relevanta pressreleaser")

    # 4. Berika delta
    if delta and press:
        delta = enrich_delta(delta, press)
        n_conf = sum(len(d["confirmed"]) for d in delta.values())
        if n_conf:
            print(f"  {n_conf} bekräftade flytt-signaler")

    # 5. Bygg HTML
    build_html(data, delta)

    # 6. Spara snapshot
    snap = {"date": TODAY, "data": {f["slug"]: f["tenants"] for f in data}}
    SNAP_FILE.write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Snapshot sparad: {SNAP_FILE}")
    print("\nKlar!")


if __name__ == "__main__":
    main()
