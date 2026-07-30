#!/usr/bin/env python3
"""
ingest_sermons.py  --  MarioGPT sermon transcript ingestion

WHAT THIS DOES
    Reads the Good News Ocala podcast RSS feed, pulls the publisher-provided
    SRT transcript for each episode, strips the caption timing junk down to
    clean prose, and uploads it to your Azure Blob "corpus" container using
    the same folder layout you already built by hand:

        corpus/sermons/2026/Modern American Problems/01-success-instead-of-gods-will.txt

    It also maintains a "latest" pointer for whole-transcript retrieval:

        corpus/latest/latest.txt     full prose of newest sermon
        corpus/latest/latest.json    title, date, series, scripture, links

WHERE THIS RUNS
    Anywhere Python runs. Locally for the one-time gap fill, GitHub Actions
    on a cron for the weekly job. Nothing about it is machine-specific.

TYPICAL USE
    python ingest_sermons.py --dry-run --backfill            # see what it'd do
    python ingest_sermons.py --backfill --since 2026-02-08   # fill the gap
    python ingest_sermons.py                                 # weekly: newest only

    # stage to local disk instead of Azure, for manual upload:
    python ingest_sermons.py --backfill --since 2026-02-01 --out-dir ./staged

ENVIRONMENT
    AZURE_STORAGE_CONNECTION_STRING   required unless --dry-run or --out-dir
    BLOB_CONTAINER                    default: corpus
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import unicodedata
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

try:
    import requests
except ImportError:
    sys.exit("Run:  pip install requests azure-storage-blob")

FEED_URL = "https://podcast.goodnewsocala.com/feed.xml"
CONTAINER = os.environ.get("BLOB_CONTAINER", "corpus")
ROOT_PREFIX = "sermons"
UA = {"User-Agent": "MarioGPT-ingest/1.0 (+https://mariogpt.com)"}
TIMEOUT = 60

# --- Microsoft Graph / OneDrive publish (optional) -------------------------
# Set these to push the newest transcript to a fixed OneDrive path where
# Cowork can read it. If any of the four credentials are unset the upload is
# skipped and the rest of the run is unaffected, so local runs need no setup.
GRAPH_ROOT = "https://graph.microsoft.com/v1.0"
GRAPH_TENANT = os.environ.get("GRAPH_TENANT_ID")
GRAPH_CLIENT = os.environ.get("GRAPH_CLIENT_ID")
GRAPH_SECRET = os.environ.get("GRAPH_CLIENT_SECRET")
ONEDRIVE_UPN = os.environ.get("ONEDRIVE_UPN")
ONEDRIVE_FOLDER = os.environ.get("ONEDRIVE_FOLDER", "Community Group")
ONEDRIVE_FILE = os.environ.get("ONEDRIVE_FILE", "latest-sermon.txt")

# Podbean emits "application/srt", which is NOT in the Podcasting 2.0 spec
# (spec lists text/plain, text/html, text/vtt, application/json,
# application/x-subrip). Match permissively and sniff the body anyway.
TYPE_PREFERENCE = [
    "text/plain", "application/json", "text/vtt",
    "application/x-subrip", "application/srt", "text/srt", "text/html",
]

# --------------------------------------------------------------------------
# regex
# --------------------------------------------------------------------------

_TC = (r"(?:\d+:)?\d{1,2}:\d{2}[.,]\d{1,3}\s*-->\s*"
       r"(?:\d+:)?\d{1,2}:\d{2}[.,]\d{1,3}")
TIMECODE = re.compile(r"^\s*" + _TC)      # anchored, per-line cleaning
TIMECODE_SNIFF = re.compile(_TC)          # unanchored, format detection
SEQ_ONLY = re.compile(r"^\s*\d+\s*$")
TAGS = re.compile(r"</?[a-zA-Z][^>]*>")
BRACED = re.compile(r"\{[^}]*\}")
CUE_SETTINGS = re.compile(r"\b(?:align|position|line|size|region):\S+")
SPEAKER = re.compile(r"^\s*(?:[A-Z][A-Za-z.'\- ]{0,28}|SPEAKER\s*\d+)\s*:\s+")
WS = re.compile(r"\s+")

QUOTES = "\"'\u201c\u201d\u2018\u2019"
# Matches:  Part 14 of "From Abraham to Joseph."
SERIES_RE = re.compile(
    rf"Part\s+(\d+)\s+of\s+[{QUOTES}]([^{QUOTES}]+)[{QUOTES}]",
    re.IGNORECASE,
)
# Same idea, but tolerates a week where whoever wrote the description left the
# quotation marks off. Lower confidence, so it gets flagged in the run log.
SERIES_LOOSE_RE = re.compile(
    rf"Part\s+(\d+)\s+of\s+[{QUOTES}]?([^.!?\n{QUOTES}]{{2,60}})",
    re.IGNORECASE,
)
# "This description mentions a part number somewhere" -- used only to detect
# that the publisher changed their wording and we are now mis-filing sermons.
PART_HINT_RE = re.compile(r"\bPart\s+\d+\b", re.IGNORECASE)

WARNINGS: list[str] = []


def warn(msg: str) -> None:
    WARNINGS.append(msg)
    print(f"       WARNING: {msg}")
BOOKS = (r"Genesis|Exodus|Leviticus|Numbers|Deuteronomy|Joshua|Judges|Ruth|"
         r"1?\s?2?\s?Samuel|1?\s?2?\s?Kings|1?\s?2?\s?Chronicles|Ezra|Nehemiah|"
         r"Esther|Job|Psalms?|Proverbs|Ecclesiastes|Song of Solomon|Isaiah|"
         r"Jeremiah|Lamentations|Ezekiel|Daniel|Hosea|Joel|Amos|Obadiah|Jonah|"
         r"Micah|Nahum|Habakkuk|Zephaniah|Haggai|Zechariah|Malachi|Matthew|"
         r"Mark|Luke|John|Acts|Romans|1?\s?2?\s?Corinthians|Galatians|"
         r"Ephesians|Philippians|Colossians|1?\s?2?\s?Thessalonians|"
         r"1?\s?2?\s?Timothy|Titus|Philemon|Hebrews|James|1?\s?2?\s?Peter|"
         r"1?\s?2?\s?3?\s?John|Jude|Revelation")
SCRIPTURE_RE = re.compile(
    rf"\b((?:[123]\s*)?(?:{BOOKS}))\s+(\d+[:\d\-\u2013,\s]*(?:\d)?)",
    re.IGNORECASE,
)

# --------------------------------------------------------------------------
# feed
# --------------------------------------------------------------------------

def local(tag: str) -> str:
    """Strip namespace. Podbean has shipped more than one namespace URI for
    the podcast prefix over the years, so match on local-name only."""
    return tag.split("}", 1)[-1].lower()


def parse_pubdate(raw: str) -> datetime:
    """RFC 2822 -> aware datetime. Returns epoch on failure so a malformed
    date sorts last rather than crashing the run."""
    try:
        dt = parsedate_to_datetime(raw)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)


def strip_html(s: str) -> str:
    s = re.sub(r"(?is)<(script|style).*?</\1>", " ", s or "")
    s = re.sub(r"(?i)</(p|div|br|li|h[1-6])>", " ", s)
    s = TAGS.sub(" ", s)
    for ent, ch in [("&nbsp;", " "), ("&amp;", "&"), ("&lt;", "<"),
                    ("&gt;", ">"), ("&quot;", '"'), ("&#39;", "'"),
                    ("&#8217;", "\u2019"), ("&#8220;", "\u201c"),
                    ("&#8221;", "\u201d")]:
        s = s.replace(ent, ch)
    return WS.sub(" ", s).strip()


def load_overrides(path: str | None) -> dict:
    """Hand-maintained corrections for episodes whose description does not
    follow the 'Part N of "Series"' convention. Keyed on episode URL (stable),
    with episode title accepted as a fallback key."""
    if not path or not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    out = {}
    for k, v in raw.items():
        if k.startswith("_") or not isinstance(v, dict):
            continue
        out[k.strip().rstrip("/").lower()] = v
    print(f"Overrides : {len(out)} loaded from {path}")
    return out


def apply_override(ep: dict, overrides: dict) -> None:
    if not overrides:
        return
    for key in (ep.get("link") or "", ep.get("title") or ""):
        v = overrides.get(key.strip().rstrip("/").lower())
        if not v:
            continue
        if "series" in v:
            s = v["series"]
            ep["series"] = s.strip() if isinstance(s, str) and s.strip() else None
        if "part" in v:
            ep["part"] = int(v["part"]) if v["part"] is not None else None
        if v.get("scripture"):
            ep["scripture"] = v["scripture"]
        ep["series_match"] = "override"
        return


def parse_episodes(root: ET.Element, overrides: dict | None = None) -> list[dict]:
    channel = root.find("channel")
    if channel is None:
        raise ValueError("feed has no <channel> element")

    eps = []
    for item in channel.findall("item"):
        desc = strip_html(item.findtext("description") or "")
        ep = {
            "title": (item.findtext("title") or "Untitled").strip(),
            "pubdate_raw": (item.findtext("pubDate") or "").strip(),
            "description": desc,
            "link": (item.findtext("link") or "").strip(),
            "audio": None,
            "transcripts": [],
        }
        ep["pubdate"] = parse_pubdate(ep["pubdate_raw"])

        enc = item.find("enclosure")
        if enc is not None:
            ep["audio"] = enc.get("url")

        for child in item:
            if local(child.tag) == "transcript" and child.get("url"):
                ep["transcripts"].append({
                    "url": child.get("url"),
                    "type": (child.get("type") or "").lower().strip(),
                })

        # Series + part number, e.g.  Part 14 of "From Abraham to Joseph."
        m = SERIES_RE.search(desc)
        if m:
            ep["part"], ep["series"] = int(m.group(1)), m.group(2)
            ep["series_match"] = "quoted"
        else:
            m2 = SERIES_LOOSE_RE.search(desc)
            if m2:
                ep["part"], ep["series"] = int(m2.group(1)), m2.group(2)
                ep["series_match"] = "loose"
            else:
                ep["part"], ep["series"] = None, None
                ep["series_match"] = "none"

        if ep["series"]:
            cleaned = ep["series"].strip().strip(QUOTES).strip().rstrip(".").strip()
            ep["series"] = cleaned or None
            if not ep["series"]:
                ep["series_match"] = "none"

        # If the description clearly talks about a part number but we could not
        # turn it into a series, the publisher's wording has drifted and this
        # sermon is about to be silently dumped into Standalone.
        ep["part_hint"] = bool(PART_HINT_RE.search(desc))

        sm = SCRIPTURE_RE.search(desc)
        ep["scripture"] = (WS.sub(" ", f"{sm.group(1)} {sm.group(2)}").strip()
                           .rstrip(",-\u2013") if sm else None)

        apply_override(ep, overrides or {})
        eps.append(ep)

    eps.sort(key=lambda e: e["pubdate"], reverse=True)   # never trust feed order
    return eps


def pick_transcript(ep: dict) -> dict | None:
    if not ep["transcripts"]:
        return None
    for want in TYPE_PREFERENCE:
        for t in ep["transcripts"]:
            if t["type"] == want:
                return t
    return ep["transcripts"][0]


# --------------------------------------------------------------------------
# transcript cleaning
# --------------------------------------------------------------------------

def timed_text_to_prose(raw: str) -> str:
    """Flatten SRT or WebVTT into continuous prose. Handles sequence numbers,
    timecodes, WebVTT headers/NOTE blocks, inline markup, positioning braces,
    cue settings, speaker labels, and consecutive duplicate cues."""
    lines = raw.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    cues, skip = [], False
    for line in lines:
        s = line.strip()
        if not s:
            skip = False
            continue
        if s.upper().startswith("WEBVTT"):
            continue
        if s.startswith(("NOTE", "STYLE", "REGION")):
            skip = True
            continue
        if skip or TIMECODE.match(s) or SEQ_ONLY.match(s):
            continue
        s = SPEAKER.sub("", CUE_SETTINGS.sub("", BRACED.sub("", TAGS.sub("", s)))).strip()
        if not s:
            continue
        if cues and cues[-1].lower() == s.lower():   # rolling caption dupes
            continue
        cues.append(s)
    text = WS.sub(" ", " ".join(cues)).strip()
    return re.sub(r"\s+([,.;:!?])", r"\1", text)


def to_prose(raw_bytes: bytes, mime: str) -> str:
    raw = raw_bytes.decode("utf-8-sig", errors="replace")
    head = raw.lstrip()[:400]
    if "json" in mime or head.startswith(("{", "[")):
        try:
            data = json.loads(raw)
            segs = data.get("segments", data if isinstance(data, list) else [])
            return WS.sub(" ", " ".join(s.get("body", "") for s in segs)).strip()
        except Exception:
            pass
    if "html" in mime or head.lower().startswith(("<!doctype", "<html")):
        return strip_html(raw)
    # Sniff rather than trust the declared type: Podbean says
    # "application/srt", which matches nothing in the spec.
    if TIMECODE_SNIFF.search(raw) or head.upper().startswith("WEBVTT"):
        return timed_text_to_prose(raw)
    return WS.sub(" ", raw).strip()


# --------------------------------------------------------------------------
# naming
# --------------------------------------------------------------------------

def slugify(title: str) -> str:
    s = unicodedata.normalize("NFKD", title)
    s = s.encode("ascii", "ignore").decode("ascii").lower()
    return (re.sub(r"[^a-z0-9]+", "-", s).strip("-") or "untitled")[:80]


def safe_folder(name: str) -> str:
    """Keep human-readable folder names (spaces preserved, like your
    'Modern American Problems'), minus characters awkward in URLs."""
    s = unicodedata.normalize("NFKD", name)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r'[\\/:*?"<>|#%]', "", s)
    return WS.sub(" ", s).strip()[:80] or "Untitled Series"


def blob_path(ep: dict, index: dict | None = None) -> str:
    """Mirrors the layout already in the container:
         sermons/2026/Modern American Problems/01-slug.txt
       Standalone sermons (no 'Part N of ...') get date-prefixed instead:
         sermons/2026/Standalone/2026-07-19_slug.txt
       The year and folder spelling come from the series index, not from this
       episode, so a series that crosses New Year's stays in one folder.
    """
    slug = slugify(ep["title"])
    if ep["series"]:
        entry = (index or {}).get(series_key(ep["series"]))
        year = entry["year"] if entry else ep["pubdate"].strftime("%Y")
        folder = entry["folder"] if entry else safe_folder(ep["series"])
        prefix = f"{ep['part']:02d}-" if ep["part"] else ""
        return f"{ROOT_PREFIX}/{year}/{folder}/{prefix}{slug}.txt"
    year = ep["pubdate"].strftime("%Y")
    date = ep["pubdate"].strftime("%Y-%m-%d")
    return f"{ROOT_PREFIX}/{year}/Standalone/{date}_{slug}.txt"


def series_key(name: str) -> str:
    """Match key that ignores case, punctuation and spacing, so 'from abraham
    to joseph' and 'From Abraham to Joseph' are recognised as one series."""
    s = unicodedata.normalize("NFKD", name or "")
    s = s.encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[^a-z0-9]+", "", s)


def build_series_index(eps: list[dict], existing_paths: set) -> dict:
    """One canonical (year, folder) per series.

    Two failure modes this prevents:
      1. A series running Dec -> Jan splitting across two year folders.
      2. A one-off typo or casing change spawning a duplicate folder.

    Blobs already in the container always win, so nothing that has already been
    uploaded gets orphaned by a later spelling change in the feed.
    """
    index: dict[str, dict] = {}

    for path in existing_paths:
        parts = path.split("/")
        if len(parts) < 4 or parts[0] != ROOT_PREFIX:
            continue
        year, folder = parts[1], parts[2]
        if folder == "Standalone" or not year.isdigit():
            continue
        k = series_key(folder)
        if k and k not in index:
            index[k] = {"year": year, "folder": folder, "source": "container"}

    # A series belongs to the year it STARTED, and takes the spelling used by
    # its earliest episode.
    earliest: dict[str, dict] = {}
    for ep in eps:
        if not ep.get("series"):
            continue
        k = series_key(ep["series"])
        if not k:
            continue
        prev = earliest.get(k)
        if prev is None or ep["pubdate"] < prev["pubdate"]:
            earliest[k] = {"pubdate": ep["pubdate"], "series": ep["series"]}

    for k, v in earliest.items():
        if k not in index:
            index[k] = {"year": v["pubdate"].strftime("%Y"),
                        "folder": safe_folder(v["series"]), "source": "feed"}
    return index


def ascii_meta(value: str, limit: int = 250) -> str:
    """Azure rejects non-ASCII blob metadata with HTTP 400 InvalidMetadata.
    Sermon titles are full of curly apostrophes, so this is not optional."""
    s = unicodedata.normalize("NFKD", str(value or ""))
    for a, b in [("\u2019", "'"), ("\u2018", "'"), ("\u201c", '"'),
                 ("\u201d", '"'), ("\u2014", "-"), ("\u2013", "-"),
                 ("\u2026", "...")]:
        s = s.replace(a, b)
    s = s.encode("ascii", "ignore").decode("ascii")
    return WS.sub(" ", re.sub(r"[\x00-\x1f\x7f]", " ", s)).strip()[:limit]


# --------------------------------------------------------------------------
# blob
# --------------------------------------------------------------------------

class Store:
    def __init__(self, dry_run: bool, out_dir: str | None = None,
                 sidecars: bool = True):
        self.dry = dry_run
        self.out_dir = out_dir
        self.sidecars = sidecars
        self.client = None
        self._existing = None
        if dry_run:
            return
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
            return
        conn = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if not conn:
            sys.exit("AZURE_STORAGE_CONNECTION_STRING is not set. "
                     "Use --dry-run or --out-dir to run without it.")
        from azure.storage.blob import BlobServiceClient
        svc = BlobServiceClient.from_connection_string(conn)
        self.client = svc.get_container_client(CONTAINER)
        try:
            self.client.create_container()
        except Exception:
            pass

    def existing(self) -> set:
        if self._existing is None:
            if self.out_dir:
                found = set()
                base = os.path.join(self.out_dir, ROOT_PREFIX)
                for dirpath, _, files in os.walk(base):
                    for f in files:
                        if f.endswith(".txt"):
                            rel = os.path.relpath(
                                os.path.join(dirpath, f), self.out_dir)
                            found.add(rel.replace(os.sep, "/"))
                self._existing = found
            elif self.dry:
                self._existing = set()
            else:
                self._existing = {
                    b.name for b in self.client.list_blobs(
                        name_starts_with=ROOT_PREFIX + "/")
                }
        return self._existing

    def write(self, path, text, meta, content_type="text/plain"):
        payload = text.encode("utf-8")
        clean = {k: ascii_meta(v) for k, v in meta.items() if v}
        if self.dry:
            print(f"       would write  {path}   ({len(payload):,} bytes)")
            return
        if self.out_dir:
            # The .txt tree is kept pure so it can be uploaded wholesale into a
            # container an indexer points at. Metadata goes into a parallel
            # _meta/ tree instead of sitting next to the transcripts.
            dest = os.path.join(self.out_dir, path.replace("/", os.sep))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as fh:
                fh.write(payload)
            if self.sidecars:
                side = os.path.join(self.out_dir, "_meta",
                                    path.replace("/", os.sep) + ".json")
                os.makedirs(os.path.dirname(side), exist_ok=True)
                with open(side, "w", encoding="utf-8") as fh:
                    json.dump(clean, fh, indent=2)
            self.existing().add(path)
            print(f"       staged  {path}   ({len(payload):,} bytes)")
            return
        from azure.storage.blob import ContentSettings
        self.client.upload_blob(
            name=path, data=payload, overwrite=True, metadata=clean,
            content_settings=ContentSettings(
                content_type=f"{content_type}; charset=utf-8"),
        )
        print(f"       wrote  {path}   ({len(payload):,} bytes)")


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# Microsoft Graph / OneDrive
# --------------------------------------------------------------------------

def graph_configured() -> bool:
    return all([GRAPH_TENANT, GRAPH_CLIENT, GRAPH_SECRET, ONEDRIVE_UPN])


def graph_token() -> str:
    """Client-credentials flow. This is app-only auth: there is no signed-in
    user, which is why the Graph permission must be an Application permission
    with admin consent granted."""
    r = requests.post(
        f"https://login.microsoftonline.com/{GRAPH_TENANT}/oauth2/v2.0/token",
        data={
            "client_id": GRAPH_CLIENT,
            "client_secret": GRAPH_SECRET,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        },
        timeout=TIMEOUT,
    )
    if r.status_code != 200:
        raise RuntimeError(f"token request failed HTTP {r.status_code}: "
                           f"{r.text[:400]}")
    return r.json()["access_token"]


def header_block(ep: dict, words: int) -> str:
    """Plain-text header so whatever reads this file knows which sermon it is
    without a second metadata file to fetch."""
    lines = [
        f"Title: {ep['title']}",
        f"Date: {ep['pubdate']:%Y-%m-%d}",
    ]
    if ep.get("series"):
        part = f" (Part {ep['part']})" if ep.get("part") else ""
        lines.append(f"Series: {ep['series']}{part}")
    else:
        lines.append("Series: Standalone")
    if ep.get("scripture"):
        lines.append(f"Scripture: {ep['scripture']}")
    if ep.get("link"):
        lines.append(f"Episode: {ep['link']}")
    lines += [
        f"Words: {words:,}",
        f"Retrieved: {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC",
        "-" * 60,
        "",
    ]
    return "\n".join(lines)


def onedrive_publish(ep: dict, prose: str, words: int) -> None:
    """Upload the newest transcript to a FIXED OneDrive path, overwriting the
    same item every week.

    Graph's PUT .../root:/path:/content replaces the content of an existing
    item and preserves its driveItem id, so any sharing link on that file keeps
    working. Deleting and recreating would mint a new id and break the link.
    """
    if not graph_configured():
        print("\nOneDrive: not configured, skipping "
              "(set GRAPH_TENANT_ID / GRAPH_CLIENT_ID / GRAPH_CLIENT_SECRET / "
              "ONEDRIVE_UPN to enable)")
        return

    body = (header_block(ep, words) + prose).encode("utf-8")
    path = f"{ONEDRIVE_FOLDER}/{ONEDRIVE_FILE}".strip("/")
    # Graph wants the path segment URL-encoded, but the ':' delimiters raw.
    encoded = "/".join(quote(seg, safe="") for seg in path.split("/"))
    url = (f"{GRAPH_ROOT}/users/{quote(ONEDRIVE_UPN)}/drive/root:"
           f"/{encoded}:/content")

    try:
        token = graph_token()
        r = requests.put(
            url,
            headers={"Authorization": f"Bearer {token}",
                     "Content-Type": "text/plain; charset=utf-8"},
            data=body, timeout=TIMEOUT,
        )
    except Exception as e:
        warn(f"OneDrive upload failed: {e.__class__.__name__}: {e}")
        return

    if r.status_code in (200, 201):
        item = r.json()
        print(f"\nOneDrive: {'updated' if r.status_code == 200 else 'created'} "
              f"/{path}  ({len(body):,} bytes)")
        print(f"          driveItem id {item.get('id')}")
        if r.status_code == 201:
            print("          NOTE: this created a NEW file. If you had shared "
                  "a link to a previous\n          file at this path, reshare "
                  "from this one -- links follow the item id.")
    else:
        warn(f"OneDrive upload failed HTTP {r.status_code}: {r.text[:300]}")


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def audit(eps, store, index) -> None:
    """Compare what the feed says SHOULD be in the container against what is
    actually there. Report only -- this never deletes anything."""
    expected = {blob_path(e, index): e for e in eps if e["transcripts"]}
    present = store.existing()

    missing = sorted(set(expected) - present)
    extra = sorted(present - set(expected))

    print("\n" + "=" * 72)
    print("AUDIT")
    print("=" * 72)
    print(f"expected from feed : {len(expected)}")
    print(f"present in target  : {len(present)}")

    print(f"\nMISSING ({len(missing)}) -- in the feed, not in the target:")
    for p in missing:
        ep = expected[p]
        print(f"  + {p}\n      {ep['pubdate']:%Y-%m-%d}  {ep['title'][:50]}")
    if not missing:
        print("  (none)")

    print(f"\nUNEXPECTED ({len(extra)}) -- in the target, not derivable "
          f"from the feed:")
    for p in extra:
        print(f"  ? {p}")
    if not extra:
        print("  (none)")

    if extra:
        print("\n  READ THIS BEFORE DELETING ANYTHING.")
        print("  This list is NOT a delete list. It legitimately includes:")
        print("    - transcripts from the old Azure Speech pipeline "
              "(the '-audio.txt' ones)")
        print("    - .jsonl provenance files")
        print("    - episodes that have aged out of the 100-item feed window")
        print("    - genuine orphans left behind by a series/part correction")
        print("  Only that last category is safe to remove, and only after you")
        print("  confirm its replacement appears in the MISSING list above.")


def handle(ep, store, latest, force, index=None) -> str:
    date_str = ep["pubdate"].strftime("%Y-%m-%d")
    head = f"{date_str}  {ep['title'][:52]}"

    t = pick_transcript(ep)
    if t is None:
        print(f"  --  {head}\n       no transcript published yet, skipping")
        return "skipped"

    path = blob_path(ep, index)
    if not force and path in store.existing():
        print(f"  ==  {head}\n       already in container, skipping")
        # The archive copy is done, but OneDrive still needs this week's
        # transcript -- otherwise a rerun after a manual upload would leave
        # the Cowork copy stale. Only relevant when publishing to Graph
        # directly; the Power Automate path re-copies latest/ on its own.
        if latest and graph_configured() and not store.dry and not store.out_dir:
            try:
                r = requests.get(t["url"], headers=UA, timeout=TIMEOUT)
                r.raise_for_status()
                prose = to_prose(r.content, t["type"])
                onedrive_publish(ep, prose, len(prose.split()))
            except requests.RequestException as e:
                warn(f"OneDrive refresh skipped, download failed: {e}")
        return "exists"

    try:
        r = requests.get(t["url"], headers=UA, timeout=TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"  !!  {head}\n       download failed: {e}")
        return "failed"

    prose = to_prose(r.content, t["type"])
    words = len(prose.split())
    if words < 100:
        print(f"  !!  {head}\n       only {words} words after cleaning, skipping")
        return "failed"

    series = ep["series"] or "Standalone"
    detail = series
    if ep["part"]:
        detail += f", part {ep['part']}"
    if ep["scripture"]:
        detail += f", {ep['scripture']}"
    print(f"  OK  {head}\n       {detail}"
          f"\n       {words:,} words  (~{int(words * 1.35):,} tokens)")

    if ep.get("series_match") == "loose":
        warn(f"{date_str} '{ep['title'][:40]}': series parsed without quotes "
             f"-> '{series}'. Check the folder is right.")
    elif ep.get("part_hint") and not ep["series"]:
        warn(f"{date_str} '{ep['title'][:40]}': description mentions a part "
             f"number but no series could be read. Filed as Standalone.")

    meta = {
        "title": ep["title"],
        "sermon_date": date_str,
        "series": series,
        "part": str(ep["part"]) if ep["part"] else "",
        "scripture": ep["scripture"] or "",
        "episode_url": ep["link"],
        "transcript_source": t["url"],
        "audio_url": ep["audio"] or "",
        "word_count": str(words),
        "source": "rss-podbean",
        "ingested_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    store.write(path, prose, meta)

    if latest:
        # The latest/ copy carries a plain-text header so whatever consumes it
        # (Power Automate -> OneDrive -> Cowork) knows which sermon it is
        # without a second file. The archive copy under sermons/ stays header
        # free: it is chunked by the search indexer, where a header would only
        # ever land in the first chunk anyway.
        store.write("latest/latest.txt", header_block(ep, words) + prose, meta)
        store.write("latest/latest.json", json.dumps({
            "title": ep["title"],
            "sermon_date": date_str,
            "series": series,
            "part": ep["part"],
            "scripture": ep["scripture"],
            "description": ep["description"][:1000],
            "episode_url": ep["link"],
            "audio_url": ep["audio"],
            "transcript_url": t["url"],
            "word_count": words,
            "approx_tokens": int(words * 1.35),
            "archive_blob": path,
            "ingested_utc": meta["ingested_utc"],
        }, indent=2, ensure_ascii=False),
            {"sermon_date": date_str, "title": ep["title"], "series": series},
            content_type="application/json")
        if not store.dry and not store.out_dir:
            onedrive_publish(ep, prose, words)
    return "ok"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true",
                   help="show what would happen, write nothing")
    p.add_argument("--backfill", action="store_true",
                   help="walk the whole feed instead of just the newest episode")
    p.add_argument("--since", metavar="YYYY-MM-DD",
                   help="only episodes published on/after this date")
    p.add_argument("--force", action="store_true",
                   help="re-upload even if the blob already exists")
    p.add_argument("--out-dir", metavar="DIR",
                   help="stage files to a local folder instead of Azure. "
                        "Blob metadata is saved alongside each file as "
                        "<name>.txt.meta.json; a drag-and-drop upload will not "
                        "apply it, so use azcopy or re-run against Azure if "
                        "you need the metadata in the container.")
    p.add_argument("--run-log", metavar="FILE", default="last-run.json",
                   help="write a run summary here (default: last-run.json)")
    p.add_argument("--recent", type=int, default=4, metavar="N",
                   help="check the N newest episodes, not just the newest one "
                        "(default 4). Anything already uploaded is skipped, so "
                        "a sermon whose transcript was late still gets picked "
                        "up on a later run.")
    p.add_argument("--overrides", metavar="FILE", default="series_overrides.json",
                   help="JSON corrections for episodes whose description does "
                        "not follow the 'Part N of \"Series\"' convention "
                        "(default: series_overrides.json if present)")
    p.add_argument("--audit", action="store_true",
                   help="report-only: compare the feed against the target and "
                        "list missing / unexpected files. Writes nothing.")
    p.add_argument("--no-sidecar", action="store_true",
                   help="with --out-dir, skip the _meta/ metadata tree")
    p.add_argument("--limit", type=int)
    p.add_argument("--feed", default=FEED_URL)
    a = p.parse_args()

    dest = a.out_dir if a.out_dir else f"container '{CONTAINER}'"
    print(f"Feed      : {a.feed}")
    print(f"Target    : {dest}{'   (DRY RUN)' if a.dry_run else ''}\n")

    r = requests.get(a.feed, headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    eps = parse_episodes(ET.fromstring(r.content), load_overrides(a.overrides))

    have = sum(1 for e in eps if e["transcripts"])
    print(f"{len(eps)} episodes in feed, {have} with transcripts "
          f"({100 * have / max(len(eps), 1):.0f}%)")
    print(f"Newest: {eps[0]['title']}  ({eps[0]['pubdate']:%Y-%m-%d})\n")

    targets = eps if a.backfill else eps[:max(a.recent, 1)]
    if a.since:
        cut = datetime.strptime(a.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        targets = [e for e in targets if e["pubdate"] >= cut]
        print(f"Filtered to {len(targets)} episodes on/after {a.since}\n")
    if a.limit:
        targets = targets[:a.limit]

    store = Store(a.dry_run, a.out_dir, sidecars=not a.no_sidecar)
    # Index is built from the WHOLE feed, not just the targets, so a partial
    # backfill still files into the folder the series already established.
    index = build_series_index(eps, store.existing())

    if a.audit:
        audit(eps, store, index)
        return

    tally = {"ok": 0, "skipped": 0, "failed": 0, "exists": 0}
    # Only the genuinely newest transcript-bearing episode in the WHOLE feed may
    # write the latest/ pointer. Without this, a run that finds the newest
    # sermon already uploaded would fall through and point latest.txt at an
    # older sermon.
    latest_ep = next((e for e in eps if e["transcripts"]), None)

    for ep in targets:
        res = handle(ep, store, ep is latest_ep, a.force, index)
        tally[res] += 1

    print(f"\nwrote={tally['ok']}  already-there={tally['exists']}  "
          f"no-transcript={tally['skipped']}  failed={tally['failed']}")

    if WARNINGS:
        print(f"\n{len(WARNINGS)} warning(s) -- review before trusting the "
              f"folder layout:")
        for w in WARNINGS:
            print(f"  - {w}")

    if a.run_log and not a.dry_run:
        newest = eps[0]
        summary = {
            "last_run_utc": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"),
            "feed": a.feed,
            "mode": "backfill" if a.backfill else "latest",
            "target": a.out_dir or CONTAINER,
            "episodes_in_feed": len(eps),
            "episodes_with_transcripts": have,
            "newest_episode": {
                "title": newest["title"],
                "pubdate": newest["pubdate"].strftime("%Y-%m-%d"),
                "series": newest["series"] or "Standalone",
                "part": newest["part"],
            },
            "result": tally,
            "warnings": WARNINGS,
        }
        with open(a.run_log, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2, ensure_ascii=False)
        print(f"\nRun log: {a.run_log}")

    if tally["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
