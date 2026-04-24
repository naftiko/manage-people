#!/usr/bin/env python3
"""Sync all Dex contacts into context/people/{letter}.yml shards.

Fetches every page of /v1/contacts from https://api.prod.getdex.com, normalizes
each contact into a schema.org Person, shards by first letter of last name,
and merges into context/people/{a-z}.yml (others go to context/people/_other.yml).

Dedup order, per shard:
  1. `identifier: "dex:{id}"` — same Dex contact re-synced
  2. LinkedIn URL match in `sameAs`
  3. Normalized `name` + normalized `worksFor.name` match

When a match is found, existing fields are preserved (email, sources, extra sameAs)
and Dex-derived fields are merged in (jobTitle, worksFor, linkedin, etc.).
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import unicodedata
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path

import yaml

PROJECT_ROOT = Path("/Users/kinlane/GitHub/naftiko-capabilities")
ENV_FILE = PROJECT_ROOT / ".env"
PEOPLE_DIR = PROJECT_ROOT / "context/people"
SUMMARY_FILE = PROJECT_ROOT / "capabilities/manage-people/data/dex-last-sync.json"

BASE_URL = "https://api.prod.getdex.com"
PAGE_SIZE = 500


def load_env_key(name: str) -> str:
    for line in ENV_FILE.read_text().splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit(f"{name} not found in {ENV_FILE}")


def http_get(path: str, token: str, params: dict | None = None) -> dict:
    url = BASE_URL + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "naftiko-manage-dex/1.0",
        },
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            raise
        except Exception:
            if attempt < 2:
                time.sleep(1)
                continue
            raise
    raise RuntimeError("unreachable")


def paginate_contacts(token: str, limit_pages: int | None = None):
    cursor: str | None = None
    page = 0
    while True:
        params = {"limit": PAGE_SIZE}
        if cursor:
            params["cursor"] = cursor
        data = http_get("/v1/contacts", token, params=params)
        page += 1
        items = data.get("data", {}).get("items") or []
        for it in items:
            yield it
        cursor = data.get("data", {}).get("nextCursor")
        if not cursor:
            return
        if limit_pages and page >= limit_pages:
            return


# ── Normalization helpers ─────────────────────────────────────────────


def strip_accents(s: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn"
    )


def shard_letter(last_name: str, full_name: str) -> str:
    name = last_name or full_name or ""
    name = strip_accents(name).strip().lower()
    if not name:
        return "_other"
    first = name[0]
    if "a" <= first <= "z":
        return first
    return "_other"


def norm_name(s: str) -> str:
    if not s:
        return ""
    s = strip_accents(s).lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def linkedin_url(slug: str | None) -> str | None:
    if not slug:
        return None
    slug = slug.strip()
    if not slug:
        return None
    if slug.startswith("http"):
        return slug.rstrip("/") + ("/" if not slug.endswith("/") else "")
    # Dex stores slug like "kinlane" or sometimes "linkedin.com/in/kinlane"
    slug = slug.replace("https://", "").replace("http://", "")
    slug = slug.replace("www.linkedin.com/in/", "").replace("linkedin.com/in/", "")
    slug = slug.strip("/").strip()
    if not slug:
        return None
    return f"https://www.linkedin.com/in/{slug}/"


def twitter_url(raw: str | None) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    if raw.startswith("http"):
        return raw
    raw = raw.replace("twitter.com/", "").replace("x.com/", "").strip("@").strip("/")
    if not raw:
        return None
    return f"https://twitter.com/{raw}"


def profile_url(raw: str | None, prefix: str) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    if raw.startswith("http"):
        return raw
    return prefix + raw.strip("/").strip("@")


def contact_to_person(c: dict) -> dict:
    first = (c.get("first_name") or "").strip()
    last = (c.get("last_name") or "").strip()
    full = (c.get("full_name") or f"{first} {last}").strip() or first or last
    same_as = []
    li = linkedin_url(c.get("linkedin"))
    if li:
        same_as.append(li)
    tw = twitter_url(c.get("twitter"))
    if tw:
        same_as.append(tw)
    fb = profile_url(c.get("facebook"), "https://www.facebook.com/")
    if fb:
        same_as.append(fb)
    ig = profile_url(c.get("instagram"), "https://www.instagram.com/")
    if ig:
        same_as.append(ig)
    if c.get("website"):
        same_as.append(c["website"])

    sources = ["dex"]
    if c.get("source"):
        sources.append(f"dex:{c['source']}")

    person = {
        "@context": "https://schema.org",
        "@type": "Person",
        "name": full,
        "identifier": f"dex:{c['id']}",
    }
    if c.get("job_title"):
        person["jobTitle"] = c["job_title"]
    if c.get("company"):
        person["worksFor"] = {"@type": "Organization", "name": c["company"]}
    if same_as:
        person["sameAs"] = same_as
    if c.get("description"):
        person["description"] = c["description"]
    person["source"] = ",".join(sources)
    return person


# ── Shard load/save ──────────────────────────────────────────────────


def load_shard(letter: str) -> dict:
    path = PEOPLE_DIR / f"{letter}.yml"
    if not path.exists():
        return {"people": []}
    data = yaml.safe_load(path.read_text()) or {}
    if "people" not in data:
        data["people"] = []
    return data


def save_shard(letter: str, data: dict) -> None:
    path = PEOPLE_DIR / f"{letter}.yml"
    path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True, default_flow_style=False), encoding="utf-8")


# ── Dedupe + merge ───────────────────────────────────────────────────


def index_shard(people: list[dict]) -> tuple[dict, dict, dict]:
    """Build three indexes: identifier -> idx, linkedin -> idx, name_key -> idx."""
    by_id: dict[str, int] = {}
    by_linkedin: dict[str, int] = {}
    by_name_org: dict[str, int] = {}
    for i, p in enumerate(people):
        ident = p.get("identifier")
        if isinstance(ident, str):
            by_id[ident] = i
        same_as = p.get("sameAs") or []
        if isinstance(same_as, str):
            same_as = [same_as]
        for u in same_as:
            if isinstance(u, str) and "linkedin.com/in/" in u:
                by_linkedin[normalize_linkedin(u)] = i
        name = p.get("name")
        wf = p.get("worksFor") or {}
        if isinstance(wf, list):
            wf = wf[0] if wf else {}
        org = wf.get("name") if isinstance(wf, dict) else None
        if name and org:
            by_name_org[f"{norm_name(name)}|{norm_name(org)}"] = i
    return by_id, by_linkedin, by_name_org


def normalize_linkedin(url: str) -> str:
    u = url.rstrip("/").lower()
    # match trailing .../in/<slug>
    m = re.search(r"linkedin\.com/in/([^/?#]+)", u)
    return m.group(1) if m else u


def merge_person(existing: dict, new: dict) -> dict:
    """Merge new Dex fields into existing record without clobbering richer data."""
    # preserve existing email if present
    # fill jobTitle, worksFor, description if existing is empty
    for field in ("jobTitle", "description"):
        if new.get(field) and not existing.get(field):
            existing[field] = new[field]
    if new.get("worksFor") and not existing.get("worksFor"):
        existing["worksFor"] = new["worksFor"]
    # merge sameAs
    existing_sa = existing.get("sameAs") or []
    if isinstance(existing_sa, str):
        existing_sa = [existing_sa]
    seen = {u.rstrip("/").lower() for u in existing_sa if isinstance(u, str)}
    for u in new.get("sameAs") or []:
        if u.rstrip("/").lower() not in seen:
            existing_sa.append(u)
            seen.add(u.rstrip("/").lower())
    if existing_sa:
        existing["sameAs"] = existing_sa
    # always stamp identifier (overwrite if missing) so future syncs match
    if new.get("identifier") and existing.get("identifier") != new["identifier"]:
        existing["identifier"] = new["identifier"]
    # append dex source tag
    src = existing.get("source", "")
    new_src = new.get("source", "dex")
    if src:
        parts = set(filter(None, (s.strip() for s in src.split(","))))
        parts.update(new_src.split(","))
        existing["source"] = ",".join(sorted(parts))
    else:
        existing["source"] = new_src
    return existing


# ── Main sync ────────────────────────────────────────────────────────


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-pages", type=int, default=0, help="Stop after N pages (0 = all)")
    ap.add_argument("--dry-run", action="store_true", help="Fetch but do not write")
    args = ap.parse_args()

    token = load_env_key("DEX_API_KEY")
    total_count = http_get("/v1/contacts/count", token).get("data", {}).get("count")
    print(f"Total Dex contacts: {total_count}", flush=True)

    # Group people by shard letter in memory, then write each shard once
    shard_cache: dict[str, dict] = {}
    shard_indexes: dict[str, tuple[dict, dict, dict]] = {}

    def get_shard(letter: str):
        if letter not in shard_cache:
            shard_cache[letter] = load_shard(letter)
            shard_indexes[letter] = index_shard(shard_cache[letter]["people"])
        return shard_cache[letter], shard_indexes[letter]

    added = updated = by_id_hit = by_linkedin_hit = by_name_hit = 0
    processed = 0

    for c in paginate_contacts(token, limit_pages=args.limit_pages or None):
        processed += 1
        person = contact_to_person(c)
        letter = shard_letter(c.get("last_name") or "", person["name"])
        shard, (by_id, by_linkedin, by_name_org) = get_shard(letter)

        match_idx = None
        ident = person["identifier"]
        if ident in by_id:
            match_idx = by_id[ident]
            by_id_hit += 1
        else:
            li = next(
                (u for u in (person.get("sameAs") or []) if "linkedin.com/in/" in u),
                None,
            )
            if li:
                slug = normalize_linkedin(li)
                if slug in by_linkedin:
                    match_idx = by_linkedin[slug]
                    by_linkedin_hit += 1
            if match_idx is None:
                wf = person.get("worksFor") or {}
                org = wf.get("name") if isinstance(wf, dict) else None
                if org:
                    key = f"{norm_name(person['name'])}|{norm_name(org)}"
                    if key in by_name_org:
                        match_idx = by_name_org[key]
                        by_name_hit += 1

        if match_idx is not None:
            merge_person(shard["people"][match_idx], person)
            updated += 1
            # re-index slots that might have changed
            existing = shard["people"][match_idx]
            by_id[existing["identifier"]] = match_idx
            for u in existing.get("sameAs") or []:
                if isinstance(u, str) and "linkedin.com/in/" in u:
                    by_linkedin[normalize_linkedin(u)] = match_idx
        else:
            shard["people"].append(person)
            idx = len(shard["people"]) - 1
            by_id[person["identifier"]] = idx
            for u in person.get("sameAs") or []:
                if isinstance(u, str) and "linkedin.com/in/" in u:
                    by_linkedin[normalize_linkedin(u)] = idx
            wf = person.get("worksFor") or {}
            org = wf.get("name") if isinstance(wf, dict) else None
            if org:
                by_name_org[f"{norm_name(person['name'])}|{norm_name(org)}"] = idx
            added += 1

        if processed % 500 == 0:
            print(
                f"  processed={processed} added={added} updated={updated} "
                f"(id={by_id_hit}, linkedin={by_linkedin_hit}, name={by_name_hit})",
                flush=True,
            )

    print(
        f"\nDone. processed={processed} added={added} updated={updated} "
        f"(id={by_id_hit}, linkedin={by_linkedin_hit}, name={by_name_hit})"
    )

    if args.dry_run:
        print("Dry run — no shards written.")
        return 0

    for letter, shard in shard_cache.items():
        save_shard(letter, shard)
        print(f"  wrote {PEOPLE_DIR}/{letter}.yml ({len(shard['people'])} total)")

    SUMMARY_FILE.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_FILE.write_text(
        json.dumps(
            {
                "ran_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "total_dex_contacts": total_count,
                "processed": processed,
                "added": added,
                "updated": updated,
                "dedupe_hits": {
                    "identifier": by_id_hit,
                    "linkedin": by_linkedin_hit,
                    "name_org": by_name_hit,
                },
                "shards_written": sorted(shard_cache.keys()),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Summary: {SUMMARY_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
