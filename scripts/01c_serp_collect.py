#!/usr/bin/env python3
"""
scripts/01c_serp_collect.py

Collecte d'offres depuis Google Jobs via SerpApi et sauvegarde en CSV.

Modes:
- normal: collecte complète et merge dans data/raw/offres_google_jobs.csv
- dry-run: collecte limitée (sample) et écrit data/raw/offres_google_jobs_sample.csv
  utile pour tester sans consommer beaucoup de quota.

Usage:
    export SERPAPI_KEY=xxxx
    python scripts/01c_serp_collect.py [--dry-run] [--sample-size N] [--max-per-query N] [--output PATH]

Dépendances (à ajouter si nécessaire dans requirements.txt):
    requests
    pandas
    tqdm
    python-dotenv (optionnel)
"""
from __future__ import annotations
import os
import time
import json
import hashlib
from datetime import datetime
from typing import Dict, List, Optional
import argparse
import requests
import pandas as pd
from tqdm import tqdm

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # dotenv optional
    pass

SERPAPI_KEY = os.environ.get("SERPAPI_KEY")
API_URL = "https://serpapi.com/search"

# Default configuration (modifiable)
QUERIES = {
    "France": {
        "Product Owner": "Product Owner",
        "Scrum Master": "Scrum Master",
        "Product Manager": "Product Manager",
    },
    "USA": {
        "Product Owner": "Product Owner",
        "Scrum Master": "Scrum Master",
        "Product Manager": "Product Manager",
    },
    "Allemagne": {
        "Product Owner": "Product Owner",
        "Scrum Master": "Scrum Master",
        "Product Manager": "Product Manager",
    },
    "Japon": {
        "Product Owner": "プロダクトオーナー OR Product Owner",
        "Scrum Master": "スクラムマスター OR Scrum Master",
        "Product Manager": "プロダクトマネージャー OR Product Manager",
    },
}

LOCATION_HINTS = {
    "France": {"location": "France", "hl": "fr", "gl": "fr"},
    "USA": {"location": "United States", "hl": "en", "gl": "us"},
    "Allemagne": {"location": "Deutschland", "hl": "de", "gl": "de"},
    "Japon": {"location": "Japan", "hl": "ja", "gl": "jp"},
}

# Behaviour defaults (can be overridden by CLI)
DEFAULT_MAX_PER_QUERY = 100  # normal mode
DEFAULT_PAGE_SIZE = 10
REQUEST_SLEEP = 1.0
MAX_RETRIES = 3
TIMEOUT = 30

# Output paths
DEFAULT_OUTPUT_CSV = "data/raw/offres_google_jobs.csv"
DEFAULT_SAMPLE_CSV = "data/raw/offres_google_jobs_sample.csv"

def make_id(item: Dict) -> str:
    key = item.get("apply_link") or item.get("link") or (item.get("title", "") + item.get("company_name", ""))
    return hashlib.sha1(str(key).encode("utf-8")).hexdigest()

def call_serpapi(params: Dict) -> Optional[Dict]:
    params = params.copy()
    params["api_key"] = SERPAPI_KEY
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(API_URL, params=params, timeout=TIMEOUT, headers={"User-Agent": "preprint-empirical-bot/1.0"})
            if resp.status_code == 200:
                return resp.json()
            else:
                print(f"⚠️ SerpApi returned {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            print(f"⚠️ Request error (attempt {attempt}): {e}")
        time.sleep(2 ** attempt)
    print("❌ Échec après retries.")
    return None

def extract_jobs_from_response(resp_json: Dict) -> List[Dict]:
    jobs = resp_json.get("jobs_results") or []
    return jobs if isinstance(jobs, list) else []

def normalize_job(job: Dict, role: str, country: str, language: str) -> Dict:
    normalized = {
        "id": make_id(job),
        "title": job.get("title"),
        "company": job.get("company_name"),
        "location": job.get("location"),
        "description": job.get("description"),
        "date_posted": job.get("date"),
        "source": job.get("source"),
        "apply_link": job.get("apply_link") or job.get("link"),
        "raw": json.dumps(job, ensure_ascii=False),
        "role_query": role,
        "country": country,
        "language": language,
        "retrieved_at": datetime.utcnow().isoformat() + "Z",
    }
    return normalized

def collect_for_query(query: str, location_hint: Dict, max_results: int = 20, page_size: int = DEFAULT_PAGE_SIZE) -> List[Dict]:
    collected = []
    start = 0
    pbar = tqdm(total=max_results, desc=f"Query: {query} [{location_hint.get('location')}]")
    while len(collected) < max_results:
        params = {
            "engine": "google_jobs",
            "q": query,
            "start": start,
            "num": page_size,
            "location": location_hint.get("location"),
            "hl": location_hint.get("hl", "en"),
            "gl": location_hint.get("gl", ""),
        }
        resp = call_serpapi(params)
        if not resp:
            break
        jobs = extract_jobs_from_response(resp)
        if not jobs:
            break
        for job in jobs:
            collected.append(job)
            pbar.update(1)
            if len(collected) >= max_results:
                break
        start += page_size
        time.sleep(REQUEST_SLEEP)
    pbar.close()
    return collected[:max_results]

def merge_and_save(records: List[Dict], output_path: str):
    df_new = pd.DataFrame(records)
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    if os.path.exists(output_path):
        df_old = pd.read_csv(output_path)
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=["id"]).reset_index(drop=True)
    else:
        df_combined = df_new.drop_duplicates(subset=["id"]).reset_index(drop=True)
    df_combined.to_csv(output_path, index=False)
    print(f"✅ Sauvegardé: {output_path} ({len(df_combined)} lignes au total)")

def run_collection(max_per_query: int, dry_run: bool, sample_size: int, output: Optional[str]):
    if not SERPAPI_KEY:
        print("❌ SERPAPI_KEY non configuré. Exportez SERPAPI_KEY dans votre environnement.")
        return

    # In dry-run we limit aggressively; the CLI sample_size overrides per-query cap.
    effective_max = max_per_query if not dry_run else max(1, sample_size)

    all_normalized = []
    for country, role_map in QUERIES.items():
        location_hint = LOCATION_HINTS.get(country, {"location": country, "hl": "en", "gl": ""})
        language = location_hint.get("hl", "en")
        for role, query_text in role_map.items():
            try:
                jobs = collect_for_query(query_text, location_hint, max_results=effective_max)
                for job in jobs:
                    norm = normalize_job(job, role=role, country=country, language=language)
                    all_normalized.append(norm)
                # Small pause between role queries
                time.sleep(1.0)
            except Exception as e:
                print(f"⚠️ Erreur lors de la collecte {country} / {role} : {e}")

    if not all_normalized:
        print("⚠️ Aucun enregistrement collecté.")
        return

    # If dry-run requested and sample_size smaller than collected, downsample deterministically
    df = pd.DataFrame(all_normalized)
    if dry_run:
        sample_path = output or DEFAULT_SAMPLE_CSV
        # deterministic sample: sort by id and take first N
        df_sampled = df.sort_values(by="id").head(sample_size)
        merge_and_save(df_sampled.to_dict(orient="records"), sample_path)
        print(f"🔎 Mode dry-run: écrit un échantillon de {len(df_sampled)} enregistrements dans {sample_path}")
    else:
        out_path = output or DEFAULT_OUTPUT_CSV
        merge_and_save(df.to_dict(orient="records"), out_path)

def parse_args():
    parser = argparse.ArgumentParser(description="Collecte offres Google Jobs via SerpApi (avec mode dry-run).")
    parser.add_argument("--dry-run", action="store_true", help="Activer le mode dry-run (écrire un sample CSV).")
    parser.add_argument("--sample-size", type=int, default=20, help="Nombre d'enregistrements à écrire en dry-run (défaut: 20).")
    parser.add_argument("--max-per-query", type=int, default=DEFAULT_MAX_PER_QUERY, help="Nombre max d'offres à récupérer par rôle/pays en mode normal.")
    parser.add_argument("--output", type=str, default=None, help="Chemin de sortie CSV (optionnel).")
    return parser.parse_args()

def main():
    args = parse_args()
    if args.dry_run:
        print("⚠️ Mode dry-run activé — collecte limitée et sortie sample CSV.")
    run_collection(max_per_query=args.max_per_query, dry_run=args.dry_run, sample_size=args.sample_size, output=args.output)

if __name__ == "__main__":
    main()