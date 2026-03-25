#!/usr/bin/env python3
"""
FlyttSignal — Lead-generator
Kör via GitHub Actions varje vecka.

Hämtar lediga kontorslokaler i Stockholm från Lokalguiden,
matchar adresser mot Bolagsverkets bulkfil,
och genererar leads.csv med bolag som sannolikt söker ny lokal.
"""

import csv
import io
import json
import re
import time
import zipfile
from datetime import date
from pathlib import Path

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
TODAY = date.today().isoformat()
SNAP_FILE = Path("lokalguiden_snapshot.json")
LEADS_FILE = Path("leads.csv")


# ── STEG 1: Hämta lediga lokaler från Lokalguiden ────────────────────────────

def fetch_lokalguiden_addresses():
    """Hämtar adresser till lediga kontorslokaler i Stockholm."""
    addresses = {}  # adress -> {url, area, size}
    page = 1

    while True:
        url = f"https://www.lokalguiden.se/lediga-lokaler/kontor/stockholm/?page={page}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                print(f"  Lokalguiden sida {page}: status {r.status_code}, avslutar")
                break

            soup = BeautifulSoup(r.text, "html.parser")

            # Hitta annonskort
            cards = soup.find_all("article") or soup.find_all(class_=re.compile(r"card|listing|object|result", re.I))

            if not cards:
                # Försök hitta adresser direkt via regex om kortstruktur saknas
                found = re.findall(
                    r'([A-ZÅÄÖ][a-zåäö]+(?:gatan|vägen|gränd|torget|plan|stigen|backen|allén|esplanaden))\s+(\d+)',
                    r.text
                )
                for street, number in found:
                    addr = f"{street} {number}"
                    if addr not in addresses:
                        addresses[addr] = {"url": url, "area": None, "size": None}
                print(f"  Lokalguiden sida {page}: {len(found)} adresser (regex-metod)")
                if not found:
                    break
            else:
                page_addrs = 0
                for card in cards:
                    text = card.get_text(" ", strip=True)
                    m = re.search(
                        r'([A-ZÅÄÖ][a-zåäö]+(?:gatan|vägen|gränd|torget|plan|stigen|backen|allén|esplanaden))\s+(\d+)',
                        text
                    )
                    if m:
                        addr = f"{m.group(1)} {m.group(2)}"
                        link = card.find("a")
                        obj_url = ("https://www.lokalguiden.se" + link["href"]) if link and link.get("href") else url
                        size_m = re.search(r'(\d+)\s*(?:kvm|m²|m2)', text, re.I)
                        addresses[addr] = {
                            "url": obj_url,
                            "area": "Stockholm",
                            "size": int(size_m.group(1)) if size_m else None
                        }
                        page_addrs += 1
                print(f"  Lokalguiden sida {page}: {page_addrs} adresser")
                if page_addrs == 0:
                    break

            page += 1
            time.sleep(0.5)

            if page > 50:  # säkerhetsgräns
                break

        except Exception as e:
            print(f"  Lokalguiden sida {page} fel: {e}")
            break

    return addresses


# ── STEG 2: Ladda Bolagsverkets bulkfil ──────────────────────────────────────

def load_bolagsverket():
    """
    Laddar ned SCB-bulkfilen och returnerar en dict:
    adress_lower -> [{ orgnr, namn, anstallda, bolagsform, sni }]
    """
    print("Laddar Bolagsverkets bulkfil...")
    url = "https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip"
    try:
        r = requests.get(url, headers=HEADERS, timeout=120, stream=True)
        r.raise_for_status()
    except Exception as e:
        print(f"  Fel vid nedladdning: {e}")
        return {}

    print(f"  Nedladdad: {len(r.content) / 1e6:.1f} MB")

    index = {}  # gatuadress_lower -> lista av bolag

    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        for name in zf.namelist():
            if not name.endswith(".txt"):
                continue
            print(f"  Läser {name}...")
            with zf.open(name) as f:
                reader = csv.DictReader(
                    io.TextIOWrapper(f, encoding="iso-8859-1"),
                    delimiter="\t"
                )
                for row in reader:
                    # Filtrera: Stockholm, aktiebolag, aktiv, >5 anställda
                    postort = (row.get("PostOrt") or row.get("Postort") or "").strip().upper()
                    if "STOCKHOLM" not in postort and "SOLNA" not in postort and "DANDERYD" not in postort:
                        continue

                    bolagsform = (row.get("JuridiskForm") or row.get("Juridiskform") or "").strip()
                    if "Aktiebolag" not in bolagsform and "AB" not in bolagsform:
                        continue

                    # Anställda -- kan vara numeriskt eller intervall
                    anst_raw = (row.get("AntalAnstallda") or row.get("Anstallda") or "0").strip()
                    try:
                        anst = int(anst_raw.split("-")[0].strip())
                    except Exception:
                        anst = 0
                    if anst < 5:
                        continue

                    gatuadress = (row.get("Gatuadress") or row.get("gatuadress") or "").strip()
                    if not gatuadress:
                        continue

                    key = gatuadress.lower()
                    if key not in index:
                        index[key] = []

                    index[key].append({
                        "orgnr":      row.get("PeOrgNr") or row.get("Orgnr") or "",
                        "namn":       row.get("ForetagetsNamn") or row.get("Namn") or "",
                        "anstallda":  anst,
                        "bolagsform": bolagsform,
                        "sni":        row.get("Ng1") or row.get("SNI") or "",
                        "adress":     gatuadress,
                        "postort":    postort.title(),
                    })

    total = sum(len(v) for v in index.values())
    print(f"  {len(index)} unika adresser, {total} bolag indexerade")
    return index


# ── STEG 3: Matcha och hitta nya lediga lokaler ───────────────────────────────

def match_and_find_leads(lokalguiden_addrs, bolagsverket_index):
    """
    Matchar Lokalguiden-adresser mot Bolagsverket.
    Returnerar lista av leads.
    """
    leads = []

    for addr, meta in lokalguiden_addrs.items():
        key = addr.lower()

        # Exakt matchning
        matches = bolagsverket_index.get(key, [])

        # Fuzzy: prova utan nummer om ingen exakt träff
        if not matches:
            street_only = re.sub(r'\s+\d+\w*$', '', key).strip()
            for bv_addr, bv_list in bolagsverket_index.items():
                if bv_addr.startswith(street_only) and re.search(r'\d', bv_addr):
                    matches.extend(bv_list)

        for company in matches:
            leads.append({
                "datum":        TODAY,
                "lokal_adress": addr,
                "lokal_url":    meta.get("url", ""),
                "lokal_kvm":    meta.get("size", ""),
                "bolag":        company["namn"],
                "orgnr":        company["orgnr"],
                "anstallda":    company["anstallda"],
                "sni":          company["sni"],
                "postort":      company["postort"],
            })

    # Sortera på antal anställda -- störst först
    leads.sort(key=lambda x: x["anstallda"], reverse=True)
    return leads


# ── STEG 4: Delta -- hitta nya lokaler sedan förra körning ────────────────────

def find_new_addresses(current_addrs):
    """Returnerar adresser som är nya sedan förra snapshot."""
    if not SNAP_FILE.exists():
        print("  Ingen tidigare snapshot -- alla adresser behandlas som nya")
        return set(current_addrs.keys())

    prev = set(json.loads(SNAP_FILE.read_text(encoding="utf-8")).get("addresses", []))
    curr = set(current_addrs.keys())
    new  = curr - prev
    print(f"  Snapshot: {len(prev)} tidigare, {len(curr)} nu, {len(new)} nya")
    return new


def save_snapshot(addrs):
    SNAP_FILE.write_text(
        json.dumps({"date": TODAY, "addresses": list(addrs.keys())}, ensure_ascii=False),
        encoding="utf-8"
    )


# ── STEG 5: Skriv leads.csv ───────────────────────────────────────────────────

def write_leads(leads):
    if not leads:
        print("Inga leads hittade denna körning.")
        return

    fields = ["datum", "lokal_adress", "lokal_url", "lokal_kvm",
              "bolag", "orgnr", "anstallda", "sni", "postort"]

    with open(LEADS_FILE, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(leads)

    print(f"\nleads.csv: {len(leads)} leads sparade")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"FlyttSignal Lead-generator — {TODAY}")
    print("=" * 45)

    print("\n1. Hämtar Lokalguiden...")
    all_addrs = fetch_lokalguiden_addresses()
    print(f"   Totalt: {len(all_addrs)} adresser från Lokalguiden")

    print("\n2. Identifierar nya lokaler...")
    new_addrs = find_new_addresses(all_addrs)
    new_meta  = {k: v for k, v in all_addrs.items() if k in new_addrs}
    print(f"   {len(new_meta)} nya lediga lokaler sedan förra körning")

    if not new_meta:
        print("   Inga nya lokaler -- avslutar")
        save_snapshot(all_addrs)
        return

    print("\n3. Laddar Bolagsverket...")
    bv_index = load_bolagsverket()

    if not bv_index:
        print("   Bolagsverket ej tillgängligt -- avslutar")
        return

    print("\n4. Matchar adresser...")
    leads = match_and_find_leads(new_meta, bv_index)
    print(f"   {len(leads)} leads hittade")

    print("\n5. Sparar leads.csv...")
    write_leads(leads)

    save_snapshot(all_addrs)
    print("\nKlar!")


if __name__ == "__main__":
    main()
