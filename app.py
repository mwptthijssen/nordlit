"""
NordLit – Multi-database Nordic Literature Search
app.py  ·  FastAPI backend
"""
from __future__ import annotations

import csv
import html
import io
import json
import os
import re
import unicodedata
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from opensearchpy import OpenSearch, TransportError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OPENSEARCH_URL = os.environ.get("OPENSEARCH_URL", "http://localhost:9200")
OPENSEARCH_USER = os.environ.get("OPENSEARCH_USER", "")
OPENSEARCH_PASS = os.environ.get("OPENSEARCH_PASS", "")
INDEX = os.environ.get("NORDLIT_INDEX", "nordlit")

EXCLUDED_SOURCES: set[str] = {"libris", "Libris", "libris_smart", "Libris_smart"}

# ---------------------------------------------------------------------------
# Source grouping and labels
# ---------------------------------------------------------------------------

SOURCE_LABELS: dict[str, str] = {
    "nva": "NVA (Norway)",
    "diva": "DiVA (Sweden)",
    "finna": "Finna (Finland)",
    "forskningsportal": "Forskningsportal (Denmark)",
    "helda": "Helda (Finland)",
    "journal_fi": "Journal.fi (Finland)",
    "opinvisindi": "Opin Visindi (Iceland)",
    "skemman": "Skemman (Iceland)",
    "theseus": "Theseus (Finland)",
    "tidsskrift_dk": "Tidsskrift.dk (Denmark)",
    "ojs_hi": "OJS (Iceland)",
    "openalex": "OpenAlex (Global)",
    "aalborg_open_journals": "Aalborg University Open Journals (Denmark)",
    "abo_journals": "Åbo Akademi OJS (Finland)",
    "aicvs": "Äldre I Centrum Vetenskapligt Supplement (Sweden)",
    "annals_of_representation_theory": "Annals of Representation Theory (Norway)",
    "arctic_portal_library": "Arctic Portal Library (Iceland)",
    "arctic_review": "Arctic Review on Law and Politics (Norway)",
    "arkiv_journal": "Arkiv. Tidskrift för samhällsanalys (Sweden)",
    "au_open_books": "AU Library E-books (Denmark)",
    "barnboken": "Barnboken (Sweden)",
    "bells": "BeLLS (Norway)",
    "bibsys": "BIBSYS Open Journals (Norway)",
    "boap_uib": "Bergen Open Access Publishing (Norway)",
    "cbs_open_journals": "CBS Open Journals (Denmark)",
    "drcmr_eprints": "DRCMR Eprints (Denmark)",
    "editori_helsinki": "Editori (Finland)",
    "ejedp": "European Journal of Economic Dynamics and Policy (Denmark)",
    "estetika": "Estetika (Finland)",
    "franorfon": "Revue nordique des études francophones (Sweden)",
    "geus_bulletin": "GEUS Bulletin (Denmark)",
    "gu_ojs": "University of Gothenburg OJS (Sweden)",
    "hprints": "hprints.org (Denmark)",
    "hup": "Helsinki University Press (Finland)",
    "iberoamericana": "Iberoamericana (Sweden)",
    "jdsr": "Journal of Digital Social Research (Sweden)",
    "jicc": "Journal of Intercultural Communication (Sweden)",
    "jisib": "Journal of Intelligence Studies in Business (Sweden)",
    "karib": "Karib (Sweden)",
    "landspitali_archive": "Landspitali Research Archive (Iceland)",
    "linkoping_ep": "Linköping University Electronic Press (Sweden)",
    "liu_electronic_press": "LiU Electronic Press (Sweden)",
    "lnuopen": "LnuOpen (Sweden)",
    "malmo_ojs": "OJS @ Malmö University (Sweden)",
    "maritime_commons": "Maritime Commons (Sweden)",
    "njas": "Nordic Journal of African Studies (Finland)",
    "njls": "Nordic Journal of Legal Studies (Finland)",
    "njmr": "Nordic Journal of Migration Research (Finland)",
    "nordic_academic_press": "Nordic Academic Press (Sweden)",
    "noril": "Nordic Journal of Information Literacy (Norway)",
    "novus_ebooks": "Novus E-bøker (Norway)",
    "novus_ojs": "Novus Online Tidsskrifter (Norway)",
    "ntnu_ojs": "NTNU Open Access Journals (Norway)",
    "ojlu": "OJLU – Lund University (Sweden)",
    "open_books_lund": "Open Books at Lund University (Sweden)",
    "orgprints": "Organic Eprints (Denmark)",
    "orkana": "Orkana Forlag (Norway)",
    "oslomet_journals": "OsloMet Open Access Journals (Norway)",
    "polar_research": "Polar Research (Norway)",
    "redescriptions": "Redescriptions (Finland)",
    "rural_landscapes": "Rural Landscapes (Sweden)",
    "septentrio": "Septentrio (Norway)",
    "silva_fennica_ojs": "Silva Fennica OJS (Finland)",
    "sjdr": "SJDR (Sweden)",
    "sjms": "Scandinavian Journal of Military Studies (Norway)",
    "sjwop": "SJWOP (Sweden)",
    "skriftserien_oslomet": "HiOA Skriftserien (Norway)",
    "socialmedicinsk_tidskrift": "Socialmedicinsk tidskrift (Sweden)",
    "stockholm_university_press": "Stockholm University Press (Sweden)",
    "textos_en_proceso": "Textos en Proceso (Sweden)",
    "tup": "Tampere University Press (Finland)",
    "uia_journals": "UiA Journal System (Norway)",
    "uio_fritt": "UiO FRITT (Norway)",
    "umea_journals": "Umeå University Hosted Journals (Sweden)",
    "universitetsforlaget": "Universitetsforlaget (Norway)",
    "vbri_press": "VBRI Press (Sweden)",
    "vtt_portal": "VTT Research Information Portal (Finland)",
}

PUBTYPE_LABELS: dict[str, str] = {
    "journal_article": "Journal Article",
    "review_article": "Review Article",
    "editorial": "Editorial",
    "letter_or_comment": "Letter or Comment",
    "note_or_short_communication": "Note / Short Communication",
    "registered_report": "Registered Report",
    "conference_paper": "Conference Paper",
    "conference_abstract": "Conference Abstract",
    "conference_poster": "Conference Poster",
    "conference_proceeding": "Conference Proceeding",
    "book": "Book",
    "edited_book": "Edited Book",
    "book_chapter": "Book Chapter",
    "report": "Report",
    "working_paper": "Working Paper",
    "policy_brief": "Policy Brief",
    "doctoral_dissertation": "Doctoral Dissertation",
    "licentiate_thesis": "Licentiate Thesis",
    "masters_thesis": "Master's Thesis",
    "bachelors_thesis": "Bachelor's Thesis",
    "student_thesis_unspecified": "Student Thesis (unspecified level)",
    "preprint": "Preprint",
    "dataset": "Dataset",
    "software": "Software",
    "patent": "Patent",
    "encyclopedia_entry": "Encyclopedia Entry",
    "reference_entry": "Reference Entry",
    "magazine_article": "Magazine Article",
    "newspaper_article": "Newspaper Article",
    "popular_science_article": "Popular Science Article",
    "professional_article": "Professional Article",
    "media_contribution": "Media Contribution",
    "artistic_output": "Artistic Output",
    "presentation_or_lecture": "Presentation or Lecture",
    "other": "Other",
    "unknown": "Unknown",
    "missing": "Missing",
}


def _prettify_source_key(key: str) -> str:
    key = (key or "").strip().replace("_", " ")
    return " ".join(part.capitalize() for part in key.split())


def _source_group(key: str) -> str:
    key = key or ""
    key_lc = key.lower()
    if key_lc.startswith("swepub"):
        return "SwePub (Sweden)"
    if key_lc.startswith("fi_institutions"):
        return "Finnish institutions (Finland)"
    if key_lc in {k.lower() for k in DEFAULT_SOURCE_GROUPS.get("Journal Platforms", [])}:
        return "Journal Platforms"
    return SOURCE_LABELS.get(key, SOURCE_LABELS.get(key_lc, _prettify_source_key(key)))


DEFAULT_SOURCE_GROUPS: dict[str, list[str]] = {
    "NVA (Norway)": ["nva"],
    "Polar Research (Norway)": ["polar_research"],
    "DiVA (Sweden)": ["diva"],
    "SwePub (Sweden)": ["swepub"],
    "Maritime Commons (Sweden)": ["maritime_commons"],
    "LiU Electronic Press (Sweden)": ["liu_electronic_press"],
    "Open Books at Lund University (Sweden)": ["open_books_lund"],
    "Stockholm University Press (Sweden)": ["stockholm_university_press"],
    "Forskningsportal (Denmark)": ["forskningsportal"],
    "Organic Eprints (Denmark)": ["orgprints"],
    "AU Library E-books (Denmark)": ["au_open_books"],
    "hprints.org (Denmark)": ["hprints"],
    "Finna (Finland)": ["finna"],
    "Finnish institutions (Finland)": ["fi_institutions"],
    "Theseus (Finland)": ["theseus"],
    "Helda (Finland)": ["helda"],
    "Skemman (Iceland)": ["skemman"],
    "Landspitali Research Archive (Iceland)": ["landspitali_archive"],
    "Opin Visindi (Iceland)": ["opinvisindi"],
    "Arctic Portal Library (Iceland)": ["arctic_portal_library"],
    "OpenAlex (Global)": ["openalex"],
    "Journal Platforms": [
        "abo_journals",
        "aicvs",
        "uio_fritt",
        "septentrio",
        "uia_journals",
        "umea_journals",
        "textos_en_proceso",
        "socialmedicinsk_tidskrift",
        "sjwop",
        "sjms",
        "sjdr",
        "rural_landscapes",
        "redescriptions",
        "oslomet_journals",
        "ojlu",
        "gu_ojs",
        "malmo_ojs",
        "ntnu_ojs",
        "novus_ojs",
        "njmr",
        "noril",
        "lnuopen",
        "linkoping_ep",
        "jdsr",
        "skriftserien_oslomet",
        "geus_bulletin",
        "silva_fennica_ojs",
        "estetika",
        "editori_helsinki",
        "cbs_open_journals",
        "boap_uib",
        "bells",
        "barnboken",
        "arkiv_journal",
        "arctic_review",
        "annals_of_representation_theory",
        "ojs_hi",
        "journal_fi",
        "tidsskrift_dk",
        "njls",
        "aalborg_open_journals",
    ],
}

GROUP_ORDER: list[str] = [
    "NVA (Norway)",
    "Polar Research (Norway)",
    "DiVA (Sweden)",
    "SwePub (Sweden)",
    "Maritime Commons (Sweden)",
    "LiU Electronic Press (Sweden)",
    "Open Books at Lund University (Sweden)",
    "Stockholm University Press (Sweden)",
    "Forskningsportal (Denmark)",
    "Organic Eprints (Denmark)",
    "AU Library E-books (Denmark)",
    "hprints.org (Denmark)",
    "Finna (Finland)",
    "Finnish institutions (Finland)",
    "Theseus (Finland)",
    "Helda (Finland)",
    "Skemman (Iceland)",
    "Landspitali Research Archive (Iceland)",
    "Opin Visindi (Iceland)",
    "Arctic Portal Library (Iceland)",
    "OpenAlex (Global)",
    "Journal Platforms",
]
# Country order for Journal Platforms sub-item sorting
JOURNAL_COUNTRY_ORDER: dict[str, int] = {
    "Norway":  0,
    "Sweden":  1,
    "Denmark": 2,
    "Finland": 3,
    "Iceland": 4,
}

# Map individual journal source keys to their country
JOURNAL_SOURCE_COUNTRY: dict[str, str] = {
    # Norway
    "uio_fritt": "Norway",
    "septentrio": "Norway",
    "uia_journals": "Norway",
    "oslomet_journals": "Norway",
    "ntnu_ojs": "Norway",
    "novus_ojs": "Norway",
    "noril": "Norway",
    "skriftserien_oslomet": "Norway",
    "boap_uib": "Norway",
    "bells": "Norway",
    "arctic_review": "Norway",
    "annals_of_representation_theory": "Norway",
    "sjms": "Norway",
    # Sweden
    "aicvs": "Sweden",
    "umea_journals": "Sweden",
    "textos_en_proceso": "Sweden",
    "socialmedicinsk_tidskrift": "Sweden",
    "sjwop": "Sweden",
    "sjdr": "Sweden",
    "rural_landscapes": "Sweden",
    "ojlu": "Sweden",
    "gu_ojs": "Sweden",
    "malmo_ojs": "Sweden",
    "lnuopen": "Sweden",
    "linkoping_ep": "Sweden",
    "jdsr": "Sweden",
    "barnboken": "Sweden",
    "arkiv_journal": "Sweden",
    # Denmark
    "geus_bulletin": "Denmark",
    "cbs_open_journals": "Denmark",
    "tidsskrift_dk": "Denmark",
    "aalborg_open_journals": "Denmark",
    # Finland
    "abo_journals": "Finland",
    "redescriptions": "Finland",
    "silva_fennica_ojs": "Finland",
    "estetika": "Finland",
    "editori_helsinki": "Finland",
    "njmr": "Finland",
    "njls": "Finland",
    "journal_fi": "Finland",
    # Iceland
    "ojs_hi": "Iceland",
}

GROUP_ORDER_INDEX: dict[str, int] = {label: idx for idx, label in enumerate(GROUP_ORDER)}

# ---------------------------------------------------------------------------
# HTML cleaning
# ---------------------------------------------------------------------------

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_SPACE_RE = re.compile(r"\s{2,}")


def clean_html(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(str(text))
    text = _HTML_TAG_RE.sub(" ", text)
    text = _MULTI_SPACE_RE.sub(" ", text)
    return text.strip()


# ---------------------------------------------------------------------------
# OpenSearch client
# ---------------------------------------------------------------------------

def _make_client() -> OpenSearch:
    kwargs: dict[str, Any] = {"hosts": [OPENSEARCH_URL], "timeout": 30}
    if OPENSEARCH_USER and OPENSEARCH_PASS:
        kwargs["http_auth"] = (OPENSEARCH_USER, OPENSEARCH_PASS)
    if OPENSEARCH_URL.startswith("https"):
        kwargs["use_ssl"] = True
        kwargs["verify_certs"] = False
    return OpenSearch(**kwargs)


client = _make_client()

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="NordLit Search")
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory=".")


# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------

OTHER_YEAR_RANGES = [(2027, 2030), (2033, 2099)]

def _is_other_year(year: int) -> bool:
    return any(start <= year <= end for start, end in OTHER_YEAR_RANGES)

def _other_year_range_clauses() -> list[dict]:
    return [
        {"range": {"year": {"gte": start, "lte": end}}}
        for start, end in OTHER_YEAR_RANGES
    ]

def _build_year_filters(
    year_from: int | None,
    year_to: int | None,
    years_exact: list[str],
) -> list[dict]:
    filters: list[dict] = []
    year_clauses: list[dict] = []

    if year_from or year_to:
        rc: dict[str, int] = {}
        if year_from is not None:
            rc["gte"] = year_from
        if year_to is not None:
            rc["lte"] = year_to
        year_clauses.append({"range": {"year": rc}})

    for y in years_exact:
        y_str = str(y)
        if y_str == "Other":
            # Records with a known year in the out-of-band buckets.
            year_clauses.append({"bool": {
                "should": _other_year_range_clauses(),
                "minimum_should_match": 1,
            }})
        elif y_str == "Missing":
            # Records without a year value at all.
            year_clauses.append({"bool": {"must_not": {"exists": {"field": "year"}}}})
        else:
            try:
                year_clauses.append({"term": {"year": int(y_str)}})
            except (ValueError, TypeError):
                pass

    if year_clauses:
        if len(year_clauses) == 1:
            filters.append(year_clauses[0])
        else:
            filters.append({"bool": {"should": year_clauses, "minimum_should_match": 1}})

    return filters


DEFAULT_TEXT_FIELDS = ["title^3", "abstract"]
EXACT_TEXT_FIELDS = ["title.accent^3", "abstract.accent"]
DEFAULT_SPAN_FIELDS = ["title", "abstract"]
EXACT_SPAN_FIELDS = ["title.accent", "abstract.accent"]


def _text_fields(exact_special_chars: bool = False) -> list[str]:
    return EXACT_TEXT_FIELDS if exact_special_chars else DEFAULT_TEXT_FIELDS


def _span_fields(exact_special_chars: bool = False) -> list[str]:
    return EXACT_SPAN_FIELDS if exact_special_chars else DEFAULT_SPAN_FIELDS
# Limit wildcard expansion inside quoted phrases so searches such as
# "teache* quali*" do not exceed OpenSearch's maxClauseCount.
# Higher values are more exhaustive but more likely to hit the 1024 clause limit.
SPAN_WILDCARD_REWRITE = "top_terms_64"

_QUOTED_WILDCARD_RE = re.compile(r'"([^"\\]*(?:\\.[^"\\]*)*)"')
_QUERY_TOKEN_RE = re.compile(r'"([^"\\]*(?:\\.[^"\\]*)*)"|\(|\)|\bAND\b|\bOR\b|\bNOT\b|\bNEAR/(\d+)\b|[^\s()]+', re.IGNORECASE)
_NEAR_OPERATOR_RE = re.compile(r"\bNEAR/(\d+)\b", re.IGNORECASE)
_SPAN_TERM_RE = re.compile(r"[\w*?]+", re.UNICODE)


def _has_quoted_wildcard(q: str) -> bool:
    """Return True when the user query contains a quoted phrase with * inside it."""
    return any("*" in m.group(1) for m in _QUOTED_WILDCARD_RE.finditer(q or ""))


def _has_near_operator(q: str) -> bool:
    """Return True when the user query contains NEAR/n proximity syntax."""
    return bool(_NEAR_OPERATOR_RE.search(q or ""))


def _tokenize_boolean_query(q: str) -> list[dict[str, str]]:
    """Small Boolean tokenizer used only for queries with wildcard phrases."""
    tokens: list[dict[str, str]] = []
    for m in _QUERY_TOKEN_RE.finditer(q or ""):
        raw = m.group(0)
        if raw == "(":
            tokens.append({"type": "LPAREN", "value": raw})
        elif raw == ")":
            tokens.append({"type": "RPAREN", "value": raw})
        elif raw.upper() in {"AND", "OR", "NOT"}:
            tokens.append({"type": raw.upper(), "value": raw.upper()})
        elif _NEAR_OPERATOR_RE.fullmatch(raw):
            tokens.append({"type": "NEAR", "value": raw.upper(), "distance": _NEAR_OPERATOR_RE.fullmatch(raw).group(1)})
        elif raw.startswith('"') and raw.endswith('"'):
            tokens.append({"type": "PHRASE", "value": raw[1:-1].replace(r'\"', '"')})
        else:
            tokens.append({"type": "TERM", "value": raw})
    return tokens


def _ascii_fold(text: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text or "")
        if not unicodedata.combining(ch)
    )


def _span_terms_from_phrase(phrase: str, exact_special_chars: bool = False) -> list[str]:
    """Approximate the analyzers: lowercase and split into searchable terms.

    The default fields use asciifolding, so span/wildcard clauses against
    those fields must be folded here too. Exact-special-character mode keeps
    accented characters and targets accent-sensitive subfields.
    """
    text = (phrase or "").lower()
    if not exact_special_chars:
        text = _ascii_fold(text)
    return [m.group(0) for m in _SPAN_TERM_RE.finditer(text)]


def _span_term_clause(field: str, term: str) -> dict:
    if "*" in term:
        return {
            "span_multi": {
                "match": {
                    "wildcard": {
                        field: {
                            "value": term,
                            "rewrite": SPAN_WILDCARD_REWRITE,
                        }
                    }
                }
            }
        }
    return {"span_term": {field: term}}


def _wildcard_phrase_clause(phrase: str, exact_special_chars: bool = False) -> dict:
    """
    Match a quoted phrase where any token may contain *.

    Examples:
    - "teacher qual*"        -> teacher followed by qual...
    - "teach* quality"       -> teach... followed by quality
    - "teacher qu*lity"      -> teacher followed by qu...lity
    - "teacher qual* policy" -> teacher followed by qual... followed by policy
    """
    terms = _span_terms_from_phrase(phrase, exact_special_chars)
    if not terms:
        return {"match_none": {}}

    field_clauses: list[dict] = []
    for field in _span_fields(exact_special_chars):
        clauses = [_span_term_clause(field, term) for term in terms]
        if len(clauses) == 1:
            field_clauses.append(clauses[0])
        else:
            field_clauses.append({
                "span_near": {
                    "clauses": clauses,
                    "slop": 0,
                    "in_order": True,
                }
            })

    return {"bool": {"should": field_clauses, "minimum_should_match": 1}}


def _span_sequence_clause(field: str, terms: list[str], *, slop: int = 0, in_order: bool = True) -> dict | None:
    """Build a field-specific span clause from analyzed terms."""
    if not terms:
        return None
    clauses = [_span_term_clause(field, term) for term in terms]
    if len(clauses) == 1:
        return clauses[0]
    return {
        "span_near": {
            "clauses": clauses,
            "slop": slop,
            "in_order": in_order,
        }
    }


def _span_map_from_terms(terms: list[str], exact_special_chars: bool = False) -> dict[str, dict] | None:
    spans: dict[str, dict] = {}
    for field in _span_fields(exact_special_chars):
        clause = _span_sequence_clause(field, terms, slop=0, in_order=True)
        if clause is not None:
            spans[field] = clause
    return spans or None


def _span_query_from_map(spans: dict[str, dict]) -> dict:
    return {
        "bool": {
            "should": list(spans.values()),
            "minimum_should_match": 1,
        }
    }


class _QueryPart:
    """Parsed query fragment plus optional field-specific span representation."""

    def __init__(self, query: dict, spans: dict[str, dict] | None = None):
        self.query = query
        self.spans = spans


def _query_part(query: dict, spans: dict[str, dict] | None = None) -> _QueryPart:
    return _QueryPart(query, spans)


def _term_part(term: str, exact_special_chars: bool = False) -> _QueryPart:
    terms = _span_terms_from_phrase(term, exact_special_chars)
    spans = _span_map_from_terms(terms, exact_special_chars) if terms else None
    return _query_part(_term_clause(term, exact_special_chars), spans)


def _phrase_part(phrase: str, exact_special_chars: bool = False) -> _QueryPart:
    terms = _span_terms_from_phrase(phrase, exact_special_chars)
    spans = _span_map_from_terms(terms, exact_special_chars) if terms else None
    query = _wildcard_phrase_clause(phrase, exact_special_chars) if "*" in phrase else _normal_phrase_clause(phrase, exact_special_chars)
    return _query_part(query, spans)


def _combine_and_part(left: _QueryPart, right: _QueryPart) -> _QueryPart:
    return _query_part(_combine_and(left.query, right.query), None)


def _combine_or_part(left: _QueryPart, right: _QueryPart) -> _QueryPart:
    spans: dict[str, dict] | None = None
    if left.spans and right.spans:
        spans = {}
        for field in sorted(set(left.spans.keys()) & set(right.spans.keys())):
            l_clause = left.spans.get(field)
            r_clause = right.spans.get(field)
            if l_clause and r_clause:
                spans[field] = {"span_or": {"clauses": [l_clause, r_clause]}}
        if not spans:
            spans = None
    return _query_part(_combine_or(left.query, right.query), spans)


def _combine_not_part(clause: _QueryPart) -> _QueryPart:
    return _query_part(_combine_not(clause.query), None)


def _combine_near_part(left: _QueryPart, right: _QueryPart, distance: int) -> _QueryPart:
    """Combine two span-capable fragments with unordered NEAR/n proximity."""
    distance = max(0, min(int(distance), 50))

    if not left.spans or not right.spans:
        # Graceful fallback for unsupported complex operands. This keeps the
        # page working, but only simple terms/phrases/OR-groups get true NEAR.
        return _query_part(_combine_and(left.query, right.query), None)

    spans: dict[str, dict] = {}
    for field in sorted(set(left.spans.keys()) & set(right.spans.keys())):
        l_clause = left.spans.get(field)
        r_clause = right.spans.get(field)
        if not l_clause or not r_clause:
            continue
        spans[field] = {
            "span_near": {
                "clauses": [l_clause, r_clause],
                "slop": distance,
                "in_order": False,
            }
        }

    if not spans:
        return _query_part(_combine_and(left.query, right.query), None)

    return _query_part(_span_query_from_map(spans), spans)


def _normal_phrase_clause(phrase: str, exact_special_chars: bool = False) -> dict:
    return {
        "multi_match": {
            "query": phrase,
            "fields": _text_fields(exact_special_chars),
            "type": "phrase",
        }
    }


def _term_clause(term: str, exact_special_chars: bool = False) -> dict:
    return {
        "query_string": {
            "query": term,
            "fields": _text_fields(exact_special_chars),
            "default_operator": "AND",
        }
    }


def _combine_and(left: dict, right: dict) -> dict:
    must: list[dict] = []
    for clause in (left, right):
        if "bool" in clause and set(clause["bool"].keys()).issubset({"must"}):
            must.extend(clause["bool"].get("must", []))
        else:
            must.append(clause)
    return {"bool": {"must": must}}


def _combine_or(left: dict, right: dict) -> dict:
    should: list[dict] = []
    for clause in (left, right):
        if "bool" in clause and set(clause["bool"].keys()).issubset({"should", "minimum_should_match"}):
            should.extend(clause["bool"].get("should", []))
        else:
            should.append(clause)
    return {"bool": {"should": should, "minimum_should_match": 1}}


def _combine_not(clause: dict) -> dict:
    return {"bool": {"must": [{"match_all": {}}], "must_not": [clause]}}


class _BooleanQueryParser:
    """Small parser for AND/OR/NOT, parentheses, terms, and quoted phrases."""

    def __init__(self, tokens: list[dict[str, str]], exact_special_chars: bool = False):
        self.tokens = tokens
        self.pos = 0
        self.exact_special_chars = exact_special_chars

    def _peek(self) -> dict[str, str] | None:
        if self.pos >= len(self.tokens):
            return None
        return self.tokens[self.pos]

    def _accept(self, token_type: str) -> dict[str, str] | None:
        tok = self._peek()
        if tok and tok["type"] == token_type:
            self.pos += 1
            return tok
        return None

    def parse(self) -> _QueryPart:
        if not self.tokens:
            return _query_part({"match_all": {}})
        clause = self._parse_or()
        if self._peek() is not None:
            raise ValueError("Unexpected token in query")
        return clause

    def _parse_or(self) -> _QueryPart:
        left = self._parse_and()
        while self._accept("OR"):
            right = self._parse_and()
            left = _combine_or_part(left, right)
        return left

    def _starts_primary(self, tok: dict[str, str] | None) -> bool:
        return bool(tok and tok["type"] in {"TERM", "PHRASE", "LPAREN", "NOT"})

    def _parse_and(self) -> _QueryPart:
        left = self._parse_near()
        while True:
            if self._accept("AND"):
                right = self._parse_near()
                left = _combine_and_part(left, right)
                continue
            # Preserve the app's historical default_operator=AND behavior.
            if self._starts_primary(self._peek()):
                right = self._parse_near()
                left = _combine_and_part(left, right)
                continue
            break
        return left

    def _parse_near(self) -> _QueryPart:
        left = self._parse_not()
        while True:
            tok = self._accept("NEAR")
            if not tok:
                break
            right = self._parse_not()
            left = _combine_near_part(left, right, int(tok.get("distance", "2")))
        return left

    def _parse_not(self) -> _QueryPart:
        if self._accept("NOT"):
            return _combine_not_part(self._parse_not())
        return self._parse_primary()

    def _parse_primary(self) -> _QueryPart:
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of query")

        if self._accept("LPAREN"):
            clause = self._parse_or()
            if not self._accept("RPAREN"):
                raise ValueError("Missing closing parenthesis")
            return clause

        tok = self._accept("PHRASE")
        if tok:
            return _phrase_part(tok["value"], self.exact_special_chars)

        tok = self._accept("TERM")
        if tok:
            return _term_part(tok["value"], self.exact_special_chars)

        raise ValueError("Unexpected token in query")


def _parsed_text_query(q: str, exact_special_chars: bool = False) -> dict:
    tokens = _tokenize_boolean_query(q)
    return _BooleanQueryParser(tokens, exact_special_chars).parse().query


def _text_clause(q: str, exact_special_chars: bool = False) -> list[dict]:
    q = q.strip()
    if not q:
        return [{"match_all": {}}]

    # Keep the original OpenSearch query_string behavior unless the query
    # contains a wildcard inside a quoted phrase or a NEAR/n proximity operator.
    # This avoids changing normal Boolean searches that already work.
    if not (_has_quoted_wildcard(q) or _has_near_operator(q)):
        return [{
            "query_string": {
                "query": q,
                "fields": _text_fields(exact_special_chars),
                "default_operator": "AND",
            }
        }]

    try:
        return [_parsed_text_query(q, exact_special_chars)]
    except Exception:
        # If the custom parser cannot understand a malformed query, fall back
        # to OpenSearch's original parser rather than breaking the search page.
        return [{
            "query_string": {
                "query": q,
                "fields": _text_fields(exact_special_chars),
                "default_operator": "AND",
            }
        }]



def _context_snippet(q: str, pos: int, radius: int = 34) -> str:
    """Return a compact snippet around a syntax problem."""
    q = q or ""
    start = max(0, pos - radius)
    end = min(len(q), pos + radius)
    snippet = q[start:end].strip()
    if start > 0:
        snippet = "…" + snippet
    if end < len(q):
        snippet += "…"
    return snippet


def _syntax_error(message: str, *, hint: str = "", q: str = "", pos: int = 0) -> dict[str, str]:
    snippet = _context_snippet(q, pos)
    return {
        "message": message,
        "hint": hint,
        "near": snippet,
    }


def _outside_quote_segments(q: str) -> list[tuple[str, int]]:
    """Return (segment, offset) pairs for text outside double quotes."""
    segments: list[tuple[str, int]] = []
    start = 0
    in_quote = False
    i = 0
    while i < len(q):
        ch = q[i]
        if ch == "\\":
            i += 2
            continue
        if ch == '"':
            if not in_quote:
                if start < i:
                    segments.append((q[start:i], start))
                in_quote = True
            else:
                in_quote = False
                start = i + 1
        i += 1
    if not in_quote and start < len(q):
        segments.append((q[start:], start))
    return segments


def _looks_like_boolean_typo(word: str) -> str | None:
    """Very conservative detection of common Boolean operator typos."""
    upper = word.upper()
    known = {"AND", "OR", "NOT"}
    if upper in known or upper.startswith("NEAR/"):
        return None

    common = {
        "ANDD": "AND", "AAND": "AND", "ADN": "AND",
        "ORR": "OR", "OOR": "OR",
        "NOTT": "NOT", "NTO": "NOT", "NOOT": "NOT",
        "NEA": "NEAR/#", "NEARR": "NEAR/#", "NEARX": "NEAR/#",
    }
    if upper in common:
        return common[upper]

    def edit_distance_one(a: str, b: str) -> bool:
        if abs(len(a) - len(b)) > 1:
            return False
        if len(a) == len(b):
            return sum(x != y for x, y in zip(a, b)) == 1
        if len(a) > len(b):
            a, b = b, a
        i = j = edits = 0
        while i < len(a) and j < len(b):
            if a[i] == b[j]:
                i += 1
                j += 1
            else:
                edits += 1
                if edits > 1:
                    return False
                j += 1
        return True

    # Only flag short ALL-CAPS words to avoid treating ordinary words as operator typos.
    if word.isupper() and len(upper) <= 5:
        for op in known:
            if edit_distance_one(upper, op):
                return op
    return None


def _validate_query_syntax(q: str) -> dict[str, str] | None:
    """Validate user-facing Boolean syntax before sending the query to OpenSearch."""
    q = (q or "").strip()
    if not q:
        return None

    # Quotes and parentheses are checked with quote awareness so parentheses
    # inside phrases are not treated as Boolean grouping syntax.
    in_quote = False
    quote_pos = -1
    paren_stack: list[int] = []
    i = 0
    while i < len(q):
        ch = q[i]
        if ch == "\\":
            i += 2
            continue
        if ch == '"':
            if in_quote:
                in_quote = False
                quote_pos = -1
            else:
                in_quote = True
                quote_pos = i
            i += 1
            continue
        if not in_quote:
            if ch == "(":
                paren_stack.append(i)
            elif ch == ")":
                if not paren_stack:
                    return _syntax_error(
                        "There is a closing parenthesis without a matching opening parenthesis.",
                        hint="Remove this ) or add an opening ( before the grouped expression.",
                        q=q,
                        pos=i,
                    )
                paren_stack.pop()
        i += 1

    if in_quote:
        return _syntax_error(
            "Missing closing quotation mark.",
            hint='Add a closing " after the phrase, or remove the opening quotation mark.',
            q=q,
            pos=max(quote_pos, 0),
        )

    if paren_stack:
        pos = paren_stack[-1]
        return _syntax_error(
            "Missing closing parenthesis.",
            hint="Add a closing ) after the grouped expression.",
            q=q,
            pos=pos,
        )

    # NEAR must be written as NEAR/number, not NEAR, NEAR/, NEAR2, etc.
    for segment, offset in _outside_quote_segments(q):
        for m in re.finditer(r"\bNEAR(?:/[^\s()\"]*)?", segment, re.IGNORECASE):
            raw = m.group(0)
            if not re.fullmatch(r"NEAR/\d+", raw, re.IGNORECASE):
                return _syntax_error(
                    "The NEAR operator is written incorrectly.",
                    hint="Use NEAR/number, for example: student NEAR/2 performance.",
                    q=q,
                    pos=offset + m.start(),
                )

        for m in re.finditer(r"\b[A-Za-z]{2,6}\b", segment):
            raw = m.group(0)
            suggestion = _looks_like_boolean_typo(raw)
            if suggestion:
                return _syntax_error(
                    f'Possible misspelled Boolean operator "{raw}".',
                    hint=f"Use {suggestion} instead, or put the word in quotes if you meant it as a search term.",
                    q=q,
                    pos=offset + m.start(),
                )

    tokens = _tokenize_boolean_query(q)
    if not tokens:
        return None

    binary_ops = {"AND", "OR", "NEAR"}
    for idx, tok in enumerate(tokens):
        typ = tok["type"]
        prev_tok = tokens[idx - 1] if idx > 0 else None
        next_tok = tokens[idx + 1] if idx + 1 < len(tokens) else None

        if typ in binary_ops:
            label = tok["value"]
            if prev_tok is None or prev_tok["type"] in {"AND", "OR", "NOT", "NEAR", "LPAREN"}:
                return _syntax_error(
                    f'{label} needs a search term before it.',
                    hint=f"Put a word, phrase, or closing parenthesis before {label}.",
                    q=q,
                    pos=q.upper().find(label.upper()),
                )
            if next_tok is None or next_tok["type"] in {"AND", "OR", "NEAR", "RPAREN"}:
                return _syntax_error(
                    f'{label} needs a search term after it.',
                    hint=f"Put a word, phrase, opening parenthesis, or NOT after {label}.",
                    q=q,
                    pos=q.upper().find(label.upper()),
                )

        if typ == "NOT":
            if next_tok is None or next_tok["type"] in {"AND", "OR", "NEAR", "RPAREN"}:
                return _syntax_error(
                    "NOT needs a search term after it.",
                    hint='For example: student NOT teacher, or student AND NOT teacher.',
                    q=q,
                    pos=q.upper().find("NOT"),
                )

        if typ == "LPAREN" and next_tok and next_tok["type"] == "RPAREN":
            return _syntax_error(
                "Empty parentheses are not a valid search expression.",
                hint="Add a word or phrase inside the parentheses, or remove them.",
                q=q,
                pos=q.find("()") if "()" in q else 0,
            )

    try:
        _BooleanQueryParser(tokens).parse()
    except ValueError as exc:
        return _syntax_error(
            "The search string could not be parsed.",
            hint=str(exc),
            q=q,
            pos=0,
        )

    return None


def _opensearch_error_detail(exc: TransportError) -> dict[str, str] | None:
    """Translate common OpenSearch query errors into user-facing messages."""
    text = str(exc)
    if "too many nested clauses" in text or "maxClauseCount" in text:
        return {
            "message": "The search query is too broad.",
            "hint": "Try a more specific wildcard pattern, for example qual* instead of q* or *tion*.",
            "near": "",
        }
    if "parse_exception" in text or "parsing_exception" in text or "Failed to parse query" in text:
        return {
            "message": "The search string contains syntax OpenSearch could not parse.",
            "hint": "Check quotation marks, parentheses, Boolean operators, and wildcard placement.",
            "near": "",
        }
    return None

def _unique_preserve(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _parse_source_pubtype_pairs(source_pubtypes: list[str]) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}
    for value in source_pubtypes or []:
        if not value or "::" not in value:
            continue
        source_key, pubtype_key = value.split("::", 1)
        if not source_key or not pubtype_key or source_key in EXCLUDED_SOURCES:
            continue
        mapping.setdefault(source_key, set()).add(pubtype_key)
    return mapping


def _build_source_filters(
    sources: list[str],
    pub_types: list[str],
    source_pubtypes: list[str],
) -> list[dict]:
    """Build database/document-type filters.

    This is kept separate from year filters so facet counts can be computed
    with cross-filtering: source facets ignore source selections, and year
    facets ignore year selections.
    """
    filters: list[dict] = []

    selected_sources = _unique_preserve([
        source for source in (sources or [])
        if source and source not in EXCLUDED_SOURCES
    ])
    scoped_pubtypes = _parse_source_pubtype_pairs(source_pubtypes)
    effective_sources = _unique_preserve(selected_sources + list(scoped_pubtypes.keys()))

    if effective_sources:
        if scoped_pubtypes:
            scoped_sources = set(scoped_pubtypes.keys())
            should_clauses: list[dict] = []
            unrestricted_sources = [source for source in effective_sources if source not in scoped_sources]
            if unrestricted_sources:
                should_clauses.append({"terms": {"source": unrestricted_sources}})
            for source_key in _unique_preserve(list(scoped_pubtypes.keys())):
                should_clauses.append({
                    "bool": {
                        "filter": [
                            {"term": {"source": source_key}},
                            {"terms": {"publication_type_normalized": sorted(scoped_pubtypes[source_key])}},
                        ]
                    }
                })
            if should_clauses:
                filters.append({"bool": {"should": should_clauses, "minimum_should_match": 1}})
        else:
            filters.append({"terms": {"source": effective_sources}})

    legacy_pub_types = _unique_preserve(pub_types or [])
    if legacy_pub_types and not scoped_pubtypes:
        filters.append({"terms": {"publication_type_normalized": legacy_pub_types}})

    return filters


def _bool_query_with_filters(q: str, filters: list[dict] | None = None, exact_special_chars: bool = False) -> dict:
    query: dict = {"bool": {"must": _text_clause(q, exact_special_chars)}}
    if filters:
        query["bool"]["filter"] = filters
    query["bool"]["must_not"] = [{"terms": {"source": sorted(EXCLUDED_SOURCES)}}]
    return query


def _build_query(
    q: str,
    sources: list[str],
    pub_types: list[str],
    source_pubtypes: list[str],
    year_from: int | None,
    year_to: int | None,
    years_exact: list[str],
    exact_special_chars: bool = False,
) -> dict:
    filters: list[dict] = []
    filters.extend(_build_source_filters(sources, pub_types, source_pubtypes))
    filters.extend(_build_year_filters(year_from, year_to, years_exact))
    return _bool_query_with_filters(q, filters, exact_special_chars)


def _build_db_facet_query(
    q: str,
    year_from: int | None,
    year_to: int | None,
    years_exact: list[str],
    exact_special_chars: bool = False,
) -> dict:
    """Facet query for database/document-type counts.

    Database counts should respond to the search string and year selections,
    but should not be narrowed by the currently selected database/document-type
    filters. This is the usual self-excluding facet behavior.
    """
    filters = _build_year_filters(year_from, year_to, years_exact)
    return _bool_query_with_filters(q, filters, exact_special_chars)


def _build_year_facet_query(
    q: str,
    sources: list[str],
    pub_types: list[str],
    source_pubtypes: list[str],
    exact_special_chars: bool = False,
) -> dict:
    """Facet query for year counts.

    Year counts should respond to the search string and database/document-type
    selections, but should not be narrowed by the currently selected year
    filters. This keeps the year facet stable while choosing years.
    """
    filters = _build_source_filters(sources, pub_types, source_pubtypes)
    return _bool_query_with_filters(q, filters, exact_special_chars)


def _search(q, sources, pub_types, source_pubtypes, year_from, year_to, years_exact, page, size, exact_special_chars: bool = False) -> dict:
    hits_body = {
        "query": _build_query(q, sources, pub_types, source_pubtypes, year_from, year_to, years_exact, exact_special_chars),
        "sort": [{"year": {"order": "desc", "missing": "_last"}}, "_score"],
        "from": (page - 1) * size,
        "size": size,
        "track_total_hits": True,
    }

    db_facet_body = {
        "query": _build_db_facet_query(q, year_from, year_to, years_exact, exact_special_chars),
        "size": 0,
        "aggs": {
            "sources": {"terms": {"field": "source", "size": 100}},
            "source_pubtypes": {
                "terms": {"field": "source", "size": 100},
                "aggs": {"pubtypes": {"terms": {"field": "publication_type_normalized", "size": 100}}},
            },
        },
    }

    year_facet_body = {
        "query": _build_year_facet_query(q, sources, pub_types, source_pubtypes, exact_special_chars),
        "size": 0,
        "aggs": {
            "years": {"terms": {"field": "year", "size": 200, "order": {"_key": "desc"}}},
            "years_missing": {"missing": {"field": "year"}},
        },
    }

    hits = client.search(index=INDEX, body=hits_body)
    db_facets = client.search(index=INDEX, body=db_facet_body)
    year_facets = client.search(index=INDEX, body=year_facet_body)

    aggs: dict = {}
    aggs.update(db_facets.get("aggregations", {}))
    aggs.update(year_facets.get("aggregations", {}))
    hits["aggregations"] = aggs
    return hits

# ---------------------------------------------------------------------------
# Source-group aggregation helper
# ---------------------------------------------------------------------------

def _group_sources(source_buckets, source_pubtype_buckets):
    groups: dict[str, dict[str, Any]] = {}

    for label, default_keys in DEFAULT_SOURCE_GROUPS.items():
        groups[label] = {
            "group_label": label,
            "keys": [key for key in default_keys if key not in EXCLUDED_SOURCES],
            "count": 0,
            "pubtypes": {},
        }

    for b in source_buckets:
        key = b["key"]
        if key in EXCLUDED_SOURCES:
            continue
        label = _source_group(key)
        if label not in groups:
            groups[label] = {"group_label": label, "keys": [], "count": 0, "pubtypes": {}}
        if key not in groups[label]["keys"]:
            groups[label]["keys"].append(key)
        groups[label]["count"] += b["doc_count"]

    # Track per-source counts for Journal Platforms (source label as sub-item)
    journal_platform_sources: dict[str, dict] = {}

    for b in source_pubtype_buckets:
        key = b["key"]
        if key in EXCLUDED_SOURCES:
            continue
        label = _source_group(key)
        if label not in groups:
            groups[label] = {"group_label": label, "keys": [key], "count": 0, "pubtypes": {}}
        elif key not in groups[label]["keys"]:
            groups[label]["keys"].append(key)

        if label == "Journal Platforms":
            # For Journal Platforms: sub-items are individual journals, not pub types
            source_count = sum(pt["doc_count"] for pt in b.get("pubtypes", {}).get("buckets", []))
            source_label = SOURCE_LABELS.get(key, _prettify_source_key(key))
            if key not in journal_platform_sources:
                journal_platform_sources[key] = {
                    "key": key,
                    "label": source_label,
                    "count": 0,
                }
            journal_platform_sources[key]["count"] += source_count
        else:
            for pt in b.get("pubtypes", {}).get("buckets", []):
                pt_key = pt["key"]
                if pt_key not in groups[label]["pubtypes"]:
                    groups[label]["pubtypes"][pt_key] = {
                        "key": pt_key,
                        "label": PUBTYPE_LABELS.get(pt_key, pt_key),
                        "count": 0,
                    }
                groups[label]["pubtypes"][pt_key]["count"] += pt["doc_count"]

    # Inject journal sources as the sub-items for Journal Platforms
    if "Journal Platforms" in groups and journal_platform_sources:
        groups["Journal Platforms"]["pubtypes"] = journal_platform_sources
        groups["Journal Platforms"]["show_sources"] = True

    result = []
    for g in sorted(groups.values(), key=lambda x: (GROUP_ORDER_INDEX.get(x["group_label"], 999), x["group_label"].lower())):
        if not g.get("show_sources"):
            g["pubtypes"] = sorted(
                g["pubtypes"].values(),
                key=lambda x: (-x["count"], x["label"].lower()),
            )
        else:
            g["pubtypes"] = sorted(
                g["pubtypes"].values(),
                key=lambda x: (
                    JOURNAL_COUNTRY_ORDER.get(JOURNAL_SOURCE_COUNTRY.get(x["key"], ""), 99),
                    -x["count"],
                ),
            )
        result.append(g)
    return result


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------

def _all_hits(query: dict):
    body = {
        "query": query,
        "sort": [{"year": {"order": "desc", "missing": "_last"}}],
        "size": 1000,
    }
    resp = client.search(index=INDEX, body=body, scroll="5m")
    scroll_id = resp.get("_scroll_id")
    hits = resp["hits"]["hits"]
    while hits:
        yield from hits
        resp = client.scroll(scroll_id=scroll_id, scroll="5m")
        scroll_id = resp.get("_scroll_id")
        hits = resp["hits"]["hits"]
    if scroll_id:
        try:
            client.clear_scroll(scroll_id=scroll_id)
        except Exception:
            pass


def _fmt_src(h: dict) -> dict:
    s = h["_source"]
    return {
        "title": clean_html(s.get("title", "")),
        "authors": clean_html(s.get("authors", "")),
        "year": s.get("year", ""),
        "abstract": clean_html(s.get("abstract", "")),
        "source": _source_group(s.get("source", "")),
        "pub_type": PUBTYPE_LABELS.get(
            s.get("publication_type_normalized", ""),
            s.get("publication_type_normalized", ""),
        ),
        "pub_type_raw": s.get("publication_type", ""),
        "source_url": s.get("source_url", ""),
    }


def _count_records(query: dict, ids: list[str] | None = None) -> int:
    if ids:
        return len(ids)
    try:
        return int(client.count(index=INDEX, body={"query": query}).get("count", 0))
    except Exception:
        count = 0
        for _ in _all_hits(query):
            count += 1
        return count


def _export_base_name(record_count: int) -> str:
    today = datetime.now().strftime("%d_%m_%Y")
    return f"Nordlit_{today}_{record_count}_records"


def _content_disposition(filename: str) -> str:
    return f'attachment; filename="{filename}"'


EXPORT_FIELDS = ["title", "authors", "year", "abstract", "source", "pub_type", "pub_type_raw", "source_url"]


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.get("/manual")
async def manual():
    """Serve manual.pdf for download/viewing in a new tab."""
    path = "manual.pdf"
    if not os.path.isfile(path):
        return PlainTextResponse("Manual not found.", status_code=404)
    def iter_file():
        with open(path, "rb") as f:
            while chunk := f.read(65536):
                yield chunk
    return StreamingResponse(
        iter_file(),
        media_type="application/pdf",
        headers={"Content-Disposition": 'inline; filename="NordLit_Manual.pdf"'},
    )


@app.get("/api/search")
async def search(
    q: str = Query(default=""),
    sources: list[str] = Query(default=[]),
    pub_types: list[str] = Query(default=[]),
    pub_type_pairs: list[str] = Query(default=[]),
    year_from: int | None = Query(default=None),
    year_to: int | None = Query(default=None),
    years_exact: list[str] = Query(default=[]),
    exact_special_chars: bool = Query(default=False),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=25),
):
    syntax_error = _validate_query_syntax(q)
    if syntax_error:
        raise HTTPException(status_code=400, detail=syntax_error)

    try:
        raw = _search(q, sources, pub_types, pub_type_pairs, year_from, year_to, years_exact, page, size, exact_special_chars)
    except TransportError as exc:
        detail = _opensearch_error_detail(exc)
        if detail:
            raise HTTPException(status_code=400, detail=detail) from exc
        raise

    total = raw["hits"]["total"]["value"]
    hits = raw["hits"]["hits"]

    results = []
    for h in hits:
        s = h["_source"]
        results.append({
            "id": h["_id"],
            "title": clean_html(s.get("title", "")),
            "authors": clean_html(s.get("authors", "")),
            "year": s.get("year"),
            "abstract": clean_html(s.get("abstract", "")),
            "source": s.get("source", ""),
            "source_label": _source_group(s.get("source", "")),
            "pub_type_raw": s.get("publication_type", ""),
            "pub_type_normalized": s.get("publication_type_normalized", ""),
            "pub_type_label": PUBTYPE_LABELS.get(
                s.get("publication_type_normalized", ""),
                s.get("publication_type_normalized", ""),
            ),
            "source_url": s.get("source_url", ""),
        })

    aggs = raw.get("aggregations", {})
    grouped = _group_sources(
        aggs.get("sources", {}).get("buckets", []),
        aggs.get("source_pubtypes", {}).get("buckets", []),
    )
    other_count = sum(
        b["doc_count"]
        for b in aggs.get("years", {}).get("buckets", [])
        if b.get("key") and _is_other_year(int(b["key"]))
    )
    missing_count = aggs.get("years_missing", {}).get("doc_count", 0)

    year_buckets = []
    if other_count > 0:
        year_buckets.append({"year": "Other", "count": other_count})
    if missing_count > 0:
        year_buckets.append({"year": "Missing", "count": missing_count})
    year_buckets += [
        {"year": b["key"], "count": b["doc_count"]}
        for b in aggs.get("years", {}).get("buckets", [])
        if b.get("key") and not _is_other_year(int(b["key"]))
    ]

    return {
        "total": total,
        "page": page,
        "size": size,
        "results": results,
        "facets": {
            "source_groups": grouped,
            "years": year_buckets,
        },
    }


@app.get("/api/export")
async def export(
    fmt: str = Query(default="csv"),
    q: str = Query(default=""),
    sources: list[str] = Query(default=[]),
    pub_types: list[str] = Query(default=[]),
    pub_type_pairs: list[str] = Query(default=[]),
    year_from: int | None = Query(default=None),
    year_to: int | None = Query(default=None),
    years_exact: list[str] = Query(default=[]),
    exact_special_chars: bool = Query(default=False),
    ids: list[str] = Query(default=[]),
):
    fmt = (fmt or "csv").lower()

    if ids:
        query: dict = {"ids": {"values": ids}}
    else:
        query = _build_query(q, sources, pub_types, pub_type_pairs, year_from, year_to, years_exact, exact_special_chars)

    record_count = _count_records(query, ids if ids else None)
    base_name = _export_base_name(record_count)

    def hits():
        yield from _all_hits(query)

    if fmt == "csv":
        def gen_csv():
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=EXPORT_FIELDS)
            w.writeheader()
            yield buf.getvalue()
            for h in hits():
                buf = io.StringIO()
                w = csv.DictWriter(buf, fieldnames=EXPORT_FIELDS)
                w.writerow(_fmt_src(h))
                yield buf.getvalue()

        filename = f"{base_name}.csv"
        return StreamingResponse(
            gen_csv(),
            media_type="text/csv",
            headers={"Content-Disposition": _content_disposition(filename)},
        )

    if fmt == "jsonl":
        def gen_jsonl():
            for h in hits():
                yield json.dumps(h["_source"], ensure_ascii=False) + "\n"

        filename = f"{base_name}.jsonl"
        return StreamingResponse(
            gen_jsonl(),
            media_type="application/x-ndjson",
            headers={"Content-Disposition": _content_disposition(filename)},
        )

    if fmt == "bibtex":
        def _bib(rec: dict, idx: int) -> str:
            title = rec["title"].replace("{", "").replace("}", "")
            authors = rec["authors"].replace(";", " and ")
            year = str(rec["year"]) if rec["year"] else ""
            key = re.sub(r"\W+", "", authors.split(",")[0] if authors else "anon") + year + str(idx)
            pt = rec.get("pub_type", "")
            btype = (
                "phdthesis" if ("Thesis" in pt or "Dissertation" in pt)
                else "book" if "Book" in pt
                else "inproceedings" if "Conference" in pt
                else "techreport" if "Report" in pt
                else "article"
            )
            ls = [f"@{btype}{{{key},", f"  title  = {{{title}}},"]
            if authors:
                ls.append(f"  author = {{{authors}}},")
            if year:
                ls.append(f"  year   = {{{year}}},")
            if rec.get("source_url"):
                ls.append(f"  url    = {{{rec['source_url']}}},")
            if rec.get("abstract"):
                ls.append(f"  abstract = {{{rec['abstract'].replace('{', '').replace('}', '')[:800]}}},")
            ls.append("}")
            return "\n".join(ls)

        def gen_bib():
            for i, h in enumerate(hits()):
                yield _bib(_fmt_src(h), i) + "\n\n"

        filename = f"{base_name}.bib"
        return StreamingResponse(
            gen_bib(),
            media_type="text/plain",
            headers={"Content-Disposition": _content_disposition(filename)},
        )

    if fmt == "ris":
        def _ris(rec: dict) -> str:
            pt = rec.get("pub_type", "")
            ty = (
                "THES" if ("Thesis" in pt or "Dissertation" in pt)
                else "CHAP" if "Book Chapter" in pt
                else "BOOK" if "Book" in pt
                else "CONF" if "Conference" in pt
                else "RPRT" if "Report" in pt
                else "JOUR"
            )
            ls = [f"TY  - {ty}", f"TI  - {rec['title']}"]
            for a in rec["authors"].split(";"):
                a = a.strip()
                if a:
                    ls.append(f"AU  - {a}")
            if rec["year"]:
                ls.append(f"PY  - {rec['year']}")
            if rec.get("abstract"):
                ls.append(f"AB  - {rec['abstract'][:800]}")
            if rec.get("source_url"):
                ls.append(f"UR  - {rec['source_url']}")
            ls.append(f"DP  - {rec.get('source', '')}")
            ls.append("ER  - ")
            return "\n".join(ls) + "\n\n"

        def gen_ris():
            for h in hits():
                yield _ris(_fmt_src(h))

        filename = f"{base_name}.ris"
        return StreamingResponse(
            gen_ris(),
            media_type="application/x-research-info-systems",
            headers={"Content-Disposition": _content_disposition(filename)},
        )

    if fmt == "xlsx":
        import openpyxl
        from openpyxl.styles import Font, PatternFill

        rows_buf = [_fmt_src(h) for h in hits()]
        buf = io.BytesIO()
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "NordLit Export"
        headers = ["Title", "Authors", "Year", "Abstract", "Source", "Publication Type", "Raw Type", "URL"]
        ws.append(headers)
        hfill = PatternFill("solid", fgColor="1E3A5F")
        hfont = Font(bold=True, color="FFFFFF", name="Calibri")
        for cell in ws[1]:
            cell.fill = hfill
            cell.font = hfont
        for r in rows_buf:
            ws.append([r["title"], r["authors"], r["year"], r["abstract"], r["source"], r["pub_type"], r["pub_type_raw"], r["source_url"]])
        for col in ws.columns:
            mx = max(len(str(c.value or "")) for c in col[:100])
            ws.column_dimensions[col[0].column_letter].width = min(max(mx + 2, 12), 60)
        wb.save(buf)
        buf.seek(0)
        filename = f"{base_name}.xlsx"
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": _content_disposition(filename)},
        )

    return PlainTextResponse("Unsupported format", status_code=400)
