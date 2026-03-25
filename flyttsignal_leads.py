#!/usr/bin/env python3
"""
FlyttSignal — Lead-generator (fixad version)
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}
TODAY = date.today().isoformat()
SNAP_FILE = Path("lokalguiden_snapshot.json")
LEADS_FILE = Path("leads.csv")


# ── STEG 1: Lokalguiden ───────────────────────────────────────────────────────

def fetch_lokalguiden_addresses():
    """Hämtar adresser från Lokalguiden -- hanterar att paginering ej fungerar."""
    addresses = {}

    # Lokalguiden paginerar inte via ?page= utan via offset eller annat mönster.
    # Vi hämtar startsidan och alla interna lokal-URL:er och besöker dem.
    base_urls = [
        "https://www.lokalguiden.se/lediga-lokaler/kontor/stockholm/",
        "https://www.lokalguiden.se/lediga-lokaler/kontor/solna/",
        "https://www.lokalguiden.se/lediga-lokaler/kontor/danderyd/",
        "https://www.lokalguiden.se/lediga-lokaler/kontor/nacka/",
        "https://www.lokalguiden.se/lediga-lokaler/kontor/sundbyberg/",
    ]

    for base_url in base_urls:
        try:
            r = requests.get(base_url, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text, "html.parser")

            # Hitta länkar till enskilda lokal-sidor
            lokal_links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/lokal/" in href or "/objekt/" in href:
                    full = href if href.startswith("http") else "https://www.lokalguiden.se" + href
                    lokal_links.append(full)

            lokal_links = list(set(lokal_links))
            print(f"  {base_url.split('/')[-2]}: {len(lokal_links)} lokal-länkar")

            # Besök varje lokal och hämta adress
            for link in lokal_links[:50]:  # max 50 per område
                try:
                    lr = requests.get(link, headers=HEADERS, timeout=10)
                    lsoup = BeautifulSoup(lr.text, "html.parser")
                    text = lsoup.get_text(" ")

                    m = re.search(
                        r'([A-ZÅÄÖ][a-zåäö]+(?:gatan|vägen|gränd|torget|plan|stigen|backen|allén|esplanaden))\s+(\d+)',
                        text
                    )
                    if m:
                        addr = f"{m.group(1)} {m.group(2)}"
                        size_m = re.search(r'(\d+)\s*(?:kvm|m²)', text, re.I)
                        addresses[addr] = {
                            "url": link,
                            "size": int(size_m.group(1)) if size_m else None
                        }
                    time.sleep(0.3)
                except Exception:
                    pass

            # Hämta också adresser direkt från listningssidan
            found = re.findall(
                r'([A-ZÅÄÖ][a-zåäö]+(?:gatan|vägen|gränd|torget|plan|stigen|backen|allén|esplanaden))\s+(\d+)',
                r.text
            )
            for street, number in found:
                addr = f"{street} {number}"
                if addr not in addresses:
                    addresses[addr] = {"url": base_url, "size": None}

        except Exception as e:
            print(f"  Fel {base_url}: {e}")

    print(f"  Totalt: {len(addresses)} unika adresser")
    return addresses


# ── STEG 2: Bolagsverket ──────────────────────────────────────────────────────

def load_bolagsverket():
    """Laddar SCB-bulkfilen och indexerar på gatuadress."""
    print("Laddar Bolagsverkets bulkfil...")
    url = "https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip"
    try:
        r = requests.get(url, headers=HEADERS, timeout=120, stream=True)
        r.raise_for_status()
        content = r.content
    except Exception as e:
        print(f"  Fel: {e}")
        return {}

    print(f"  Nedladdad: {len(content) / 1e6:.1f} MB")

    index = {}

    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        for name in zf.namelist():
            if not name.endswith(".txt"):
                continue
            print(f"  Läser {name}...")

            with zf.open(name) as f:
                raw = io.TextIOWrapper(f, encoding="iso-8859-1")
                reader = csv.reader(raw, delimiter="\t")

                # Läs header för att hitta kolumnnamn
                try:
                    headers_row = next(reader)
                except StopIteration:
                    continue

                print(f"  Alla kolumner ({len(headers_row)} st): {headers_row}")

                # Bygg kolumnindex
                col = {h.strip(): i for i, h in enumerate(headers_row)}

                # Hitta relevanta kolumner -- prova varianter
                def get_col(*names):
                    for n in names:
                        if n in col:
                            return col[n]
                    return None

                idx_adress   = get_col("Gatuadress", "gatuadress", "GatuAdress", "Besöksadress")
                idx_postort  = get_col("Postort", "PostOrt", "postort")
                idx_form     = get_col("JurForm", "JuridiskForm", "Juridiskform", "BolagsForm")
                idx_anst     = get_col("AntAnst", "AntalAnstallda", "Anstallda", "anstallda")
                idx_namn     = get_col("Foretagsnamn", "ForetagetsNamn", "Namn", "namn")
                idx_orgnr    = get_col("PeOrgNr", "Orgnr", "OrganisationsNr")
                idx_sni      = get_col("Ng1", "SNI", "SniKod")

                print(f"  Adress-kolumn: {idx_adress}, Postort: {idx_postort}, Form: {idx_form}, Anst: {idx_anst}")

                if idx_adress is None:
                    print("  VARNING: Hittade ingen adress-kolumn -- hoppar över fil")
                    continue

                count = 0
                for row in reader:
                    if len(row) <= max(filter(None, [idx_adress, idx_postort, idx_form, idx_anst, idx_namn])):
                        continue

                    # Postort -- filtrera Stockholm-regionen
                    postort = row[idx_postort].strip().upper() if idx_postort is not None else ""
                    sthlm_orter = {"STOCKHOLM", "SOLNA", "DANDERYD", "NACKA", "SUNDBYBERG",
                                   "LIDINGÖ", "LIDINGO", "JÄRFÄLLA", "JARFALLA", "HUDDINGE"}
                    if not any(o in postort for o in sthlm_orter):
                        continue

                    # Bolagsform
                    form = row[idx_form].strip() if idx_form is not None else ""
                    if "AB" not in form and "Aktiebolag" not in form:
                        continue

                    # Anställda
                    anst_raw = row[idx_anst].strip() if idx_anst is not None else "0"
                    try:
                        anst = int(re.sub(r'\D.*', '', anst_raw) or "0")
                    except Exception:
                        anst = 0
                    if anst < 5:
                        continue

                    gatuadress = row[idx_adress].strip()
                    if not gatuadress:
                        continue

                    key = gatuadress.lower()
                    if key not in index:
                        index[key] = []

                    index[key].append({
                        "orgnr":      row[idx_orgnr].strip() if idx_orgnr is not None else "",
                        "namn":       row[idx_namn].strip() if idx_namn is not None else "",
                        "anstallda":  anst,
                        "bolagsform": form,
                        "sni":        row[idx_sni].strip() if idx_sni is not None else "",
                        "adress":     gatuadress,
                        "postort":    postort.title(),
                    })
                    count += 1

                print(f"  {count} bolag inlästa, {len(index)} unika adresser")

    return index


# ── STEG 3: Matcha ────────────────────────────────────────────────────────────

def match(lokalguiden_addrs, bv_index):
    leads = []
    for addr, meta in lokalguiden_addrs.items():
        key = addr.lower()
        matches = bv_index.get(key, [])

        # Fuzzy: prova gatunamn utan nummer
        if not matches:
            street = re.sub(r'\s+\d+\w*$', '', key).strip()
            for bv_key, bv_list in bv_index.items():
                if re.match(re.escape(street) + r'\s+\d', bv_key):
                    matches.extend(bv_list)

        for c in matches:
            leads.append({
                "datum":        TODAY,
                "lokal_adress": addr,
                "lokal_url":    meta.get("url", ""),
                "lokal_kvm":    meta.get("size", ""),
                "bolag":        c["namn"],
                "orgnr":        c["orgnr"],
                "anstallda":    c["anstallda"],
                "sni":          c["sni"],
                "postort":      c["postort"],
            })

    leads.sort(key=lambda x: x["anstallda"], reverse=True)
    return leads


# ── STEG 4: Delta ─────────────────────────────────────────────────────────────

def find_new(current):
    if not SNAP_FILE.exists():
        print("  Ingen snapshot -- alla adresser är nya")
        return set(current.keys())
    prev = set(json.loads(SNAP_FILE.read_text(encoding="utf-8")).get("addresses", []))
    curr = set(current.keys())
    new  = curr - prev
    print(f"  {len(prev)} tidigare, {len(curr)} nu, {len(new)} nya")
    return new


def save_snapshot(addrs):
    SNAP_FILE.write_text(
        json.dumps({"date": TODAY, "addresses": list(addrs.keys())}, ensure_ascii=False),
        encoding="utf-8"
    )


# ── STEG 5: Skriv CSV ─────────────────────────────────────────────────────────

def write_csv(leads):
    if not leads:
        print("Inga leads denna körning.")
        # Skriv tom fil så artifact-steget inte klagar
        LEADS_FILE.write_text("datum,lokal_adress,lokal_url,lokal_kvm,bolag,orgnr,anstallda,sni,postort\n")
        return

    fields = ["datum","lokal_adress","lokal_url","lokal_kvm","bolag","orgnr","anstallda","sni","postort"]
    with open(LEADS_FILE, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(leads)
    print(f"leads.csv: {len(leads)} leads sparade")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"FlyttSignal — {TODAY}")
    print("=" * 40)

    print("\n1. Lokalguiden...")
    all_addrs = fetch_lokalguiden_addresses()

    print("\n2. Nya adresser...")
    new_keys  = find_new(all_addrs)
    new_addrs = {k: v for k, v in all_addrs.items() if k in new_keys}
    print(f"   {len(new_addrs)} nya")

    if not new_addrs:
        print("Inga nya lokaler.")
        save_snapshot(all_addrs)
        LEADS_FILE.write_text("datum,lokal_adress,lokal_url,lokal_kvm,bolag,orgnr,anstallda,sni,postort\n")
        return

    print("\n3. Bolagsverket...")
    bv = load_bolagsverket()

    if not bv:
        print("Bolagsverket ej tillgängligt.")
        return

    print("\n4. Matchar...")
    leads = match(new_addrs, bv)
    print(f"   {len(leads)} leads")

    print("\n5. Sparar...")
    write_csv(leads)
    save_snapshot(all_addrs)
    print("Klar!")


if __name__ == "__main__":
    main()
