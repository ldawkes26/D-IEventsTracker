"""
Events Radar — production scraper.

Runs daily via GitHub Actions. Fetches each configured source page, passes it
to Claude for structured extraction, optionally follows per-event detail
pages for richer data, deduplicates, and writes events.json to the repo.
The static webpage (public/index.html) fetches events.json and re-renders.

Environment variables:
  ANTHROPIC_API_KEY     required. Set as a GitHub Actions secret.
  OUTPUT_DIR            optional. Defaults to "public".
  FOLLOW_EVENT_LINKS    optional. "true" to enable two-pass extraction
                        (follows each event's detail URL for a richer pull).
                        Default "false". Enabling roughly triples cost.

Local test run:
  export ANTHROPIC_API_KEY=sk-ant-...
  python scraper.py
"""

import os
import json
import hashlib
import sqlite3
from datetime import datetime, date
from pathlib import Path
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from anthropic import Anthropic

OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "."))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = OUTPUT_DIR / "events.db"

# Self-heal: if events.db exists but isn't a valid SQLite file, delete it
# so a fresh one can be created. Common when the file got corrupted in
# a manual move or upload.
if DB_PATH.exists():
    try:
        import sqlite3 as _sq
        _conn = _sq.connect(DB_PATH)
        _conn.execute("SELECT 1")
        _conn.close()
    except _sq.DatabaseError:
        print(f"⚠ {DB_PATH} is corrupted — deleting so a fresh database can be created")
        DB_PATH.unlink()

FOLLOW_EVENT_LINKS = os.environ.get("FOLLOW_EVENT_LINKS", "false").lower() == "true"

# ---------------------------------------------------------------------------
# SOURCES
# ---------------------------------------------------------------------------
SOURCES = [
    # --- Investigations & financial crime ---
    {"name": "Global Investigations Review", "url": "https://globalinvestigationsreview.com/events"},
    {"name": "ACAMS", "url": "https://www.acams.org/en/events/acams-events-view-all-upcoming-events"},
    {"name": "C5 Anti-Corruption London", "url": "https://www.c5-online.com/ac-london/"},
    {"name": "C5 FCPA Portfolio", "url": "https://www.c5-online.com/conference/anti-corruption-fcpa/"},
    {"name": "Cambridge Economic Crime Symposium", "url": "https://www.crimesymposium.org/register"},
    {"name": "RUSI CFS", "url": "https://www.rusi.org/explore-our-research/research-groups/centre-for-finance-and-security"},

    # --- Arbitration institutions ---
    {"name": "London Court of International Arbitration", "url": "https://www.lcia.org/lcia-events/events_schedule.aspx"},
    {"name": "Young International Arbitration Group", "url": "https://www.lcia.org/Membership/YIAG/Young_International_Arbitration_Group.aspx"},
    {"name": "International Chamber of Commerce", "url": "https://iccwbo.org/news-publications/events/"},
    {"name": "ICC Young Arbitrators Forum", "url": "https://iccwbo.org/dispute-resolution/professional-development/young-arbitrators-forum-yaf/"},
    {"name": "ICC United Kingdom", "url": "https://iccwbo.uk/collections/events"},
    {"name": "International Council for Commercial Arbitration", "url": "https://www.arbitration-icca.org/events"},
    {"name": "Young ICCA", "url": "https://www.youngicca.org/events"},
    {"name": "Chartered Institute of Arbitrators", "url": "https://www.ciarb.org/events"},
    {"name": "Arbitral Women", "url": "https://www.arbitralwomen.org/aw-events/"},
    {"name": "Delos Dispute Resolution", "url": "https://delosdr.org/index.php/delos-events-calendar/"},

    # --- Disputes & arbitration media / weeks ---
    {"name": "Global Arbitration Review", "url": "https://globalarbitrationreview.com/events"},
    {"name": "LIDW", "url": "https://register.lidw.co.uk/"},
    {"name": "IAC London Arbitration Calendar", "url": "https://www.iac-london.com/arbitral-events-2026/"},
    {"name": "Commercial Dispute Resolution", "url": "https://www.cdr-news.com/conferences/"},
    {"name": "Thought Leaders 4", "url": "https://thoughtleaders4.com/tl4-disputes/events"},

    # --- Bar / academic ---
    {"name": "International Bar Association", "url": "https://www.ibanet.org/conferences"},
    {"name": "British Institute of International and Comparative Law", "url": "https://www.biicl.org/events"},

    # --- Law firm events ---
    {"name": "Herbert Smith Freehills", "url": "https://www.hsfkramer.com/events"},
    {"name": "Gibson Dunn", "url": "https://www.gibsondunn.com/insights/"},
    {"name": "WilmerHale", "url": "https://www.wilmerhale.com/en/insights-and-events"},
    {"name": "Freshfields Bruckhaus Deringer", "url": "https://www.freshfields.com/en/our-thinking/events"},
    {"name": "Simmons & Simmons", "url": "https://www.simmons-simmons.com/en/insights?type=events"},
    {"name": "A&O Shearman", "url": "https://seminars.aoshearman.com/seminars?event_name="},
    {"name": "Addleshaw Goddard", "url": "https://www.addleshawgoddard.com/en/events/"},
    {"name": "Clifford Chance", "url": "https://www.cliffordchance.com/insights/thought_leadership/seminars-training-and-events.html"},
    {"name": "Hogan Lovells Cadwalader", "url": "https://www.hlc.com/en/events"},
    {"name": "Linklaters (Sustainable Futures)", "url": "https://sustainablefutures.linklaters.com/tag/webinars%20%26%20events"},
    {"name": "Ropes & Gray", "url": "https://www.ropesgray.com/en/news-and-events"},
    {"name": "Skadden", "url": "https://www.skadden.com/insights?skip=0&type=0416a0ae-d175-4993-bbe0-6cf0cf7397da&hassearched=true"},
    {"name": "White & Case", "url": "https://www.whitecase.com/insights/search?type=2"},
    {"name": "Kirkland & Ellis", "url": "https://www.kirkland.com/insights?type=e47e40af-b7c0-49af-902f-eb8741bc6463"},
    {"name": "Norton Rose Fulbright", "url": "https://www.nortonrosefulbright.com/en/knowledge/webinars-and-events"},
    {"name": "Peters & Peters", "url": "https://www.petersandpeters.com/category/thinking/events/"},
    {"name": "Baker McKenzie (Global Trade)", "url": "https://www.bakermckenzie.com/en/insight/events/baker-mckenzie-global-trade-webinars-and-events"},
    {"name": "Mishcon de Reya", "url": "https://www.mishcon.com/academy/live"},
    {"name": "Covington & Burling", "url": "https://www.cov.com/en/events"},
    {"name": "Davis Polk", "url": "https://www.davispolk.com/insights/client-updates"},
    {"name": "King & Spalding", "url": "https://www.kslaw.com/news-and-insights?locale=en&post_type=1"},
    {"name": "Latham & Watkins", "url": "https://www.lw.com/en/insights-listing#sort=%40contentdate%20descending"},
    {"name": "Paul, Weiss", "url": "https://www.paulweiss.com/insights/events"},
    {"name": "Akin Gump", "url": "https://www.akingump.com/en/insights"},
    {"name": "Cleary Gottlieb", "url": "https://www.clearygottlieb.com/news-and-insights/events-listing"},
    {"name": "Sidley Austin", "url": "https://www.sidley.com/en/insights?sortCriteria=%40datesort%20descending&f-contenttype=Events&cq=%40alltemplates%3D%3D(883C0625A81743E1BA1683DAEB2C73AD)%20AND%20NOT%20%40ez120xcludez32xfromz32xsearch%3D%3D1"},
    {"name": "Simpson Thacher & Bartlett", "url": "https://www.stblaw.com/about-us/events"},
    {"name": "Jenner & Block", "url": "https://www.jenner.com/en/news-insights?nt=35920"},
    {"name": "Jones Day", "url": "https://www.jonesday.com/en/insights?tab=events"},
    {"name": "Quinn Emanuel", "url": "https://www.quinnemanuel.com/the-firm/news-events/"},
]

# ---------------------------------------------------------------------------
# EXTRACTION PROMPTS
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You extract structured event records from HTML scraped from
law firm, regulator, professional body, and conference organiser websites.

Return ONLY valid JSON — no preamble, no markdown fences, no commentary.
Return an array of events. If the page has no events, return [].

RELEVANCE: Only include events whose primary subject matter is disputes,
litigation, arbitration, mediation, internal or regulatory investigations,
financial crime (AML, sanctions, ABC/bribery, fraud, market abuse, tax
evasion, export controls), asset recovery, or enforcement.
Exclude general corporate, M&A, tax, employment, IP, real estate, ESG.

If the event's end date is before today's date, set relevant=false.

URL RULE — critical:
  - "url" must be the specific event's own page/registration link if present
    in the HTML (look for links on the event card or title, commonly in
    href attributes labelled "Register", "Read more", "View event", or the
    title itself linking to a detail page).
  - If the event has no distinct per-event URL on this page, set url to null.
    Do NOT fall back to the organiser's homepage or the source URL.
  - Relative URLs (e.g. "/events/xyz") should be resolved against the
    source URL given in the user message — return the absolute URL.

SCHEMA per event (all keys required):
{
  "title": "string — event name, no organiser suffix",
  "organiser": "string",
  "start": "YYYY-MM-DD",
  "end": "YYYY-MM-DD (same as start if single-day)",
  "city": "string or 'Virtual'",
  "country": "string or ''",
  "region": "UK | Europe | North America | APAC | MENA | LATAM | Africa | Global",
  "format": "In-person | Virtual | Hybrid",
  "topics": ["array from: Investigations, FCPA, ABC, Bribery, Fraud, AML, Sanctions, Export Controls, Financial Crime, Market Abuse, Tax Evasion, Asset Recovery, Enforcement, Disputes, Litigation, Arbitration, Mediation, Compliance, Policy, Networking"],
  "audience": "Junior | Mixed | Senior",
  "cost": "Free | Paid | Invite-only",
  "costDisplay": "string as shown on page (e.g. '£695', 'Free (members)', 'Register')",
  "url": "absolute URL of the event's own page, or null if none on this page",
  "flags": ["Flagship" if a major marquee event, otherwise empty array],
  "summary": "<=40 words, factual, plain English, no marketing language",
  "confidence": 0.0-1.0,
  "relevant": true or false
}

Today's date will be in the user message; use it to resolve relative dates."""

# Used in the optional "second pass" — fetch each individual event page
# and enrich the record with richer summary / confirmed URL.
DETAIL_SYSTEM_PROMPT = """You read the HTML of a single event's detail page and
return ONE JSON object improving the existing record.

Return ONLY valid JSON — no preamble, no markdown fences.

Schema (all keys required):
{
  "summary": "<=60 words, factual description from the detail page",
  "cost": "Free | Paid | Invite-only",
  "costDisplay": "exact price/cost string as shown on the page",
  "format": "In-person | Virtual | Hybrid",
  "city": "string or 'Virtual'",
  "country": "string or ''",
  "registration_url": "the canonical registration URL, typically where the 'Register' button leads, absolute URL"
}

If a field is not discernible, return the existing value from the input record unchanged."""

# ---------------------------------------------------------------------------
# FETCH
# ---------------------------------------------------------------------------
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}

def fetch_html(url: str, timeout: int = 30) -> str:
    """Fetch URL, strip noise, return cleaned HTML under a size cap."""
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        r = client.get(url, headers=HTTP_HEADERS)
        r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "iframe"]):
        tag.decompose()
    main = soup.find("main") or soup.find("article") or soup.body or soup
    return str(main)[:60_000]

def resolve_url(base: str, candidate):
    """Turn a relative URL into an absolute URL against base. Returns None if candidate is falsy."""
    if not candidate:
        return None
    if candidate.startswith(("http://", "https://")):
        return candidate
    return urljoin(base, candidate)

# ---------------------------------------------------------------------------
# EXTRACT (pass 1 — listing page)
# ---------------------------------------------------------------------------
def extract_events(html: str, source_url: str, client: Anthropic) -> list[dict]:
    today = date.today().isoformat()
    user_msg = f"today: {today}\nsource_url: {source_url}\n\nHTML:\n{html}"
    resp = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=4000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = resp.content[0].text.strip()
    if text.startswith("```"):
        text = text.strip("`").lstrip("json").strip()
    try:
        events = json.loads(text)
        if not isinstance(events, list):
            return []
        for ev in events:
            ev["url"] = resolve_url(source_url, ev.get("url"))
        return events
    except json.JSONDecodeError as e:
        print(f"    ! JSON parse error: {e}")
        print(f"    first 200 chars of response: {text[:200]}")
        return []

# ---------------------------------------------------------------------------
# EXTRACT (pass 2 — detail page, optional)
# ---------------------------------------------------------------------------
def enrich_from_detail(ev: dict, client: Anthropic) -> dict:
    """Fetch the event's detail page and improve the record.
    Returns the event (possibly modified). If anything fails, returns ev unchanged."""
    if not ev.get("url"):
        return ev
    try:
        html = fetch_html(ev["url"], timeout=20)
    except Exception as e:
        print(f"      · detail fetch failed for {ev['url']}: {e}")
        return ev

    user_msg = f"existing record:\n{json.dumps(ev, indent=2)}\n\nDETAIL PAGE HTML:\n{html}"
    try:
        resp = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=1200,
            system=DETAIL_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        text = resp.content[0].text.strip()
        if text.startswith("```"):
            text = text.strip("`").lstrip("json").strip()
        improvement = json.loads(text)
    except Exception as e:
        print(f"      · enrichment failed for {ev['url']}: {e}")
        return ev

    merged = dict(ev)
    for k in ("summary", "cost", "costDisplay", "format", "city", "country"):
        if improvement.get(k):
            merged[k] = improvement[k]
    if improvement.get("registration_url"):
        merged["url"] = resolve_url(ev["url"], improvement["registration_url"])
    return merged

# ---------------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    title TEXT, organiser TEXT,
    start TEXT, end TEXT,
    city TEXT, country TEXT,
    region TEXT, format TEXT,
    topics TEXT,
    audience TEXT,
    cost TEXT, costDisplay TEXT,
    url TEXT, flags TEXT,
    summary TEXT,
    source_name TEXT, source_url TEXT,
    first_seen TEXT, last_seen TEXT,
    confidence REAL
);
CREATE INDEX IF NOT EXISTS idx_start ON events(start);
"""

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    return conn

def event_id(ev: dict) -> str:
    key = f"{ev.get('title','').lower().strip()}|{ev.get('start','')}|{ev.get('city','').lower().strip()}"
    return "EVT-" + hashlib.sha1(key.encode()).hexdigest()[:10].upper()

def upsert(conn, ev: dict, source_name: str, source_url: str) -> bool:
    if not ev.get("relevant", True):
        return False
    if ev.get("confidence", 1.0) < 0.5:
        return False
    if not ev.get("title") or not ev.get("start"):
        return False

    eid = event_id(ev)
    today = date.today().isoformat()

    existing = conn.execute("SELECT first_seen FROM events WHERE id=?", (eid,)).fetchone()
    first_seen = existing[0] if existing else today

    # URL fallback chain: per-event URL (ideal) -> source URL (last resort)
    final_url = ev.get("url") or source_url

    conn.execute("""
        INSERT OR REPLACE INTO events VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, (
        eid,
        ev.get("title"), ev.get("organiser"),
        ev.get("start"), ev.get("end", ev.get("start")),
        ev.get("city"), ev.get("country", ""),
        ev.get("region"), ev.get("format"),
        json.dumps(ev.get("topics", [])),
        ev.get("audience", "Mixed"),
        ev.get("cost"), ev.get("costDisplay", ""),
        final_url, json.dumps(ev.get("flags", [])),
        ev.get("summary", ""),
        source_name, source_url, first_seen, today, ev.get("confidence", 1.0)
    ))
    return True

# ---------------------------------------------------------------------------
# EXPORT
# ---------------------------------------------------------------------------
def export_json(conn):
    rows = conn.execute("""
        SELECT id, title, organiser, start, end, city, country, region, format,
               topics, audience, cost, costDisplay, url, flags, summary
        FROM events
        WHERE end >= DATE('now','-7 days')
        ORDER BY start ASC
    """).fetchall()

    events = []
    for r in rows:
        events.append({
            "id": r[0],
            "title": r[1], "organiser": r[2],
            "start": r[3], "end": r[4],
            "city": r[5], "country": r[6],
            "region": r[7], "format": r[8],
            "topics": json.loads(r[9] or "[]"),
            "audience": r[10],
            "cost": r[11], "costDisplay": r[12],
            "url": r[13], "flags": json.loads(r[14] or "[]"),
            "summary": r[15] or "",
        })

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "event_count": len(events),
        "sources": SOURCES,
        "events": events,
    }
    out_path = OUTPUT_DIR / "events.json"
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"\n✓ Wrote {len(events)} events to {out_path}")

    import csv
    csv_path = OUTPUT_DIR / "events.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id","title","organiser","start","end","city","country","region",
                    "format","topics","audience","cost","costDisplay","url","flags","summary"])
        for e in events:
            w.writerow([
                e["id"], e["title"], e["organiser"], e["start"], e["end"],
                e["city"], e["country"], e["region"], e["format"],
                "; ".join(e["topics"]), e["audience"], e["cost"], e["costDisplay"],
                e["url"], "; ".join(e["flags"]), e["summary"]
            ])
    print(f"✓ Wrote CSV mirror to {csv_path}")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def run():
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit("ANTHROPIC_API_KEY environment variable not set")

    print(f"Events Radar scraper")
    print(f"  sources: {len(SOURCES)}")
    print(f"  follow event links: {FOLLOW_EVENT_LINKS}")
    print()

    client = Anthropic()
    conn = get_db()

    total_upserted = 0
    total_skipped = 0
    total_enriched = 0

    for i, source in enumerate(SOURCES, 1):
        print(f"[{i}/{len(SOURCES)}] {source['name']}")
        print(f"    url: {source['url']}")

        try:
            html = fetch_html(source["url"])
        except Exception as e:
            print(f"    ! fetch failed: {e}")
            continue

        try:
            events = extract_events(html, source["url"], client)
        except Exception as e:
            print(f"    ! extraction failed: {e}")
            continue

        print(f"    extracted {len(events)} candidate events")

        for ev in events:
            if not ev.get("relevant", True):
                total_skipped += 1
                continue

            if FOLLOW_EVENT_LINKS and ev.get("url"):
                ev = enrich_from_detail(ev, client)
                total_enriched += 1

            if upsert(conn, ev, source["name"], source["url"]):
                total_upserted += 1
            else:
                total_skipped += 1

        conn.commit()

    print(f"\nSummary:")
    print(f"  {total_upserted} events upserted")
    print(f"  {total_skipped} skipped (not relevant / low confidence / missing fields)")
    if FOLLOW_EVENT_LINKS:
        print(f"  {total_enriched} enriched with detail-page data")

    export_json(conn)
    conn.close()

if __name__ == "__main__":
    run()
