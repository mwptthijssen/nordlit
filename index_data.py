#!/usr/bin/env python3
"""
index_data.py
=============
Creates the NordLit OpenSearch index and bulk-indexes JSONL files from a given directory.
Uses cleaned_diva.jsonl for DiVA when present.
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import os
import re
import sys
from pathlib import Path

from opensearchpy import OpenSearch, helpers

LOG = logging.getLogger("index_data")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

INDEX_NAME = "nordlit"
BATCH_SIZE = 1000
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_SPACE_RE = re.compile(r"\s{2,}")
_ORGPRINTS_OAI_RE = re.compile(r"^oai:orgprints\.org:(\d+)$", re.IGNORECASE)
_ORGPRINTS_URL_RE = re.compile(
    r"orgprints\.org/(?:id/eprint/)?(\d+)(?:/|$|[?#])",
    re.IGNORECASE,
)
_NUMERIC_ID_RE = re.compile(r"^(\d+)$")

INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 1,
        "analysis": {
            "analyzer": {
                "nordlit_text": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding"],
                },
                "nordlit_text_exact": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"],
                }
            }
        },
    },
    "mappings": {
        "properties": {
            "title": {
                "type": "text",
                "analyzer": "nordlit_text",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "accent": {"type": "text", "analyzer": "nordlit_text_exact"},
                },
            },
            "authors": {"type": "text", "analyzer": "nordlit_text"},
            "year": {"type": "integer"},
            "abstract": {
                "type": "text",
                "analyzer": "nordlit_text",
                "fields": {
                    "accent": {"type": "text", "analyzer": "nordlit_text_exact"},
                },
            },
            "source_url": {"type": "keyword", "index": False},
            "source": {"type": "keyword"},
            "publication_type": {"type": "keyword"},
            "publication_type_normalized": {"type": "keyword"},
            "_raw_id": {"type": "keyword"},
            "pubtype_mapping_note": {"type": "keyword", "index": False},
        }
    },
}


def clean_html_text(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(str(text))
    text = _HTML_TAG_RE.sub(" ", text)
    text = _MULTI_SPACE_RE.sub(" ", text)
    return text.strip()


def _extract_orgprints_id(*values: object) -> str | None:
    """Return the Organic Eprints numeric record id from common metadata forms."""
    for value in values:
        raw = str(value or "").strip()
        if not raw:
            continue

        for pattern in (_ORGPRINTS_OAI_RE, _ORGPRINTS_URL_RE, _NUMERIC_ID_RE):
            match = pattern.search(raw)
            if match:
                return match.group(1)

    return None


def normalize_orgprints_url(source: str, source_url: str | None, raw_id: str | None) -> str:
    """Create stable Organic Eprints landing-page URLs when source metadata lacks one.

    Organic Eprints records often arrive through OAI-PMH with identifiers such as
    ``oai:orgprints.org:29427`` but no separate ``source_url``.  The web
    landing page for such a record is ``https://orgprints.org/id/eprint/29427/``.
    This normalizer only changes records whose source key contains ``orgprint``;
    all other sources keep their original URL value.
    """
    source_key = str(source or "").strip().lower()
    url = str(source_url or "").strip()

    if "orgprint" not in source_key:
        return url

    # If a usable non-Organic-Eprints URL is already present, preserve it.
    # If it is an Organic Eprints URL or OAI/numeric identifier, canonicalize it.
    record_id = _extract_orgprints_id(url, raw_id)
    if record_id:
        return f"https://orgprints.org/id/eprint/{record_id}/"

    return url


def _iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _to_doc(rec: dict) -> dict | None:
    title = clean_html_text(rec.get("title") or "")
    if not title:
        return None

    year = None
    raw_year = rec.get("year")
    if raw_year:
        try:
            y = int(str(raw_year).strip()[:4])
            if 1000 <= y <= 2100:
                year = y
        except (ValueError, TypeError):
            pass

    authors = rec.get("authors") or []
    if isinstance(authors, list):
        authors_str = "; ".join(clean_html_text(a) for a in authors if a)
    else:
        authors_str = clean_html_text(authors)

    source = (rec.get("source") or "").strip()
    raw_id = (rec.get("_raw_id") or "").strip()
    source_url = normalize_orgprints_url(source, rec.get("source_url"), raw_id)

    return {
        "title": title,
        "authors": authors_str,
        "year": year,
        "abstract": clean_html_text(rec.get("abstract") or ""),
        "source_url": source_url,
        "source": source,
        "publication_type": (rec.get("publication_type") or "").strip(),
        "publication_type_normalized": (rec.get("publication_type_normalized") or "").strip(),
        "_raw_id": raw_id,
        "pubtype_mapping_note": (rec.get("pubtype_mapping_note") or "").strip(),
    }


def _actions(path: Path, index: str):
    for rec in _iter_jsonl(path):
        doc = _to_doc(rec)
        if doc is None:
            continue
        doc_id = doc["_raw_id"] or None
        action = {"_index": index, "_source": doc}
        if doc_id:
            action["_id"] = doc_id
        yield action


def get_client(host: str, user: str | None, password: str | None) -> OpenSearch:
    kwargs: dict = {"hosts": [host], "timeout": 60}
    if user and password:
        kwargs["http_auth"] = (user, password)
    if host.startswith("https"):
        kwargs["use_ssl"] = True
        kwargs["verify_certs"] = False
    return OpenSearch(**kwargs)


def find_input_files(data_dir: Path, source: str | None) -> list[Path]:
    files = sorted(data_dir.glob("normalized_*.jsonl"))
    cleaned_diva = data_dir / "cleaned_diva.jsonl"
    normalized_diva = data_dir / "normalized_diva.jsonl"

    if cleaned_diva.exists():
        files = [f for f in files if f.name != normalized_diva.name]
        files.append(cleaned_diva)

    files = sorted(files, key=lambda p: p.name.lower())
    if source:
        files = [f for f in files if source.lower() in f.stem.lower()]
    return files


def main() -> None:
    ap = argparse.ArgumentParser(description="Index NordLit JSONL into OpenSearch.")
    ap.add_argument("--data-dir", required=True, type=Path, help="Directory containing JSONL files.")
    ap.add_argument("--host", default=os.environ.get("OPENSEARCH_URL", "http://localhost:9200"))
    ap.add_argument("--user", default=os.environ.get("OPENSEARCH_USER", ""))
    ap.add_argument("--password", default=os.environ.get("OPENSEARCH_PASS", ""))
    ap.add_argument("--index", default=INDEX_NAME)
    ap.add_argument("--batch", type=int, default=BATCH_SIZE)
    ap.add_argument("--reset", action="store_true", help="Delete and recreate the index before indexing.")
    ap.add_argument("--source", default=None, help="Only index files whose name contains this string.")
    args = ap.parse_args()

    client = get_client(args.host, args.user or None, args.password or None)

    try:
        info = client.info()
        LOG.info("Connected to OpenSearch %s", info.get("version", {}).get("number", "?"))
    except Exception as e:
        LOG.error("Cannot connect to OpenSearch at %s: %s", args.host, e)
        sys.exit(1)

    if args.reset and client.indices.exists(index=args.index):
        LOG.info("Deleting existing index '%s' ...", args.index)
        client.indices.delete(index=args.index)

    if not client.indices.exists(index=args.index):
        LOG.info("Creating index '%s' ...", args.index)
        client.indices.create(index=args.index, body=INDEX_MAPPING)
    else:
        LOG.info("Index '%s' already exists — appending.", args.index)

    files = find_input_files(args.data_dir, args.source)
    if not files:
        LOG.error("No matching JSONL files found in %s", args.data_dir)
        sys.exit(1)

    LOG.info("Found %d file(s) to index.", len(files))
    total_indexed = total_errors = 0

    for path in files:
        label = path.stem
        LOG.info("Indexing %s ...", label)
        ok, errors = helpers.bulk(client, _actions(path, args.index), chunk_size=args.batch, raise_on_error=False, stats_only=False)
        total_indexed += ok
        total_errors += len(errors) if isinstance(errors, list) else errors
        LOG.info("  %s: indexed=%d  errors=%d", label, ok, len(errors) if isinstance(errors, list) else errors)

    LOG.info("Done. Total indexed: %d  errors: %d", total_indexed, total_errors)
    client.indices.refresh(index=args.index)
    count = client.count(index=args.index)["count"]
    LOG.info("Index '%s' now contains %d documents.", args.index, count)


if __name__ == "__main__":
    main()
