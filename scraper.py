#!/usr/bin/env python3
"""
Scrape Yellow Pages via Apify and export all results.

Usage:
  --max-item set to 0 means no limit
  -l must be two charector abbreviation for api accuracy.

  python scraper.py --category "Plumber" --location "NC" -o Plumber_NC_YellowPage --max-items 260
  python scraper.py -c "Dentist" -l "CA" -o dentists_ca --max-items 0
  python scraper.py -c "Restaurant" -l "tx" --max-items 50

APIFY_TOKEN is read from the APIFY_TOKEN env var or --token.
Location must be a valid U.S. state abbreviation (e.g., NC, CA, TX, FL).
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime
from typing import Dict, Any, Iterable, Optional

from apify_client import ApifyClient


ACTOR_ID = "wWqrTazDTGHCGTFvw"  # Yellow Pages scraper actor

# U.S. state abbreviations
US_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC'  # Washington D.C.
}



def run_yellow_pages_actor(
    client: ApifyClient,
    category: str,
    location: str,
    max_items: Optional[int] = None,
    debug: bool = False,
    max_cost: float = 5.0,
) -> Dict[str, Any]:
    """Start the actor and return the run object."""
    # Build actor input; many Apify actors treat 0 or None as 'no limit'.
    run_input = {
        "search": category,
        "location": location,
        "debugMode": bool(debug),
    }
    if max_items is not None:
        run_input["maxItems"] = max_items  # 0 means unlimited for many actors

    return client.actor(ACTOR_ID).call(run_input=run_input)


def iter_results(client: ApifyClient, dataset_id: str) -> Iterable[Dict[str, Any]]:
    """Yield all items from the actor's dataset (handles pagination)."""
    for item in client.dataset(dataset_id).iterate_items():
        yield item


def normalize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten/normalize Yellow Pages listing based on actual API response structure."""
    
    # Helper function to join lists into strings
    def join_list(field):
        if isinstance(field, list):
            return ",".join(str(x) for x in field if x) if field else ""
        return field or ""
    
    # Helper function to parse address into components
    def parse_address(address_str):
        if not address_str:
            return {"street": "", "city": "", "state": "", "zip": ""}
        
        # Expected format: "1601 N Long Beach Blvd, Compton, CA 90221"
        parts = address_str.split(", ")
        if len(parts) >= 3:
            street = parts[0]
            city = parts[1]
            # Last part usually contains "STATE ZIP"
            state_zip = parts[-1].split()
            state = state_zip[0] if state_zip else ""
            zip_code = state_zip[1] if len(state_zip) > 1 else ""
            return {"street": street, "city": city, "state": state, "zip": zip_code}
        else:
            return {"street": address_str, "city": "", "state": "", "zip": ""}
    
    # Parse address components
    address_info = parse_address(rec.get("address", ""))
    
    return {
        # Core fields from actual API response
        "name": rec.get("name", ""),
        "address": rec.get("address", ""),
        "phone": rec.get("phone", ""),
        "url": rec.get("url", ""),
        "rating": rec.get("rating", ""),
        "rating_count": rec.get("ratingCount", ""),
        "categories": join_list(rec.get("categories")),
        "image": rec.get("image", ""),
        "is_ad": rec.get("isAd", ""),
        "info_snippet": rec.get("infoSnippet", ""),
        
        # Parsed address components
        "street": address_info["street"],
        "city": address_info["city"],
        "state": address_info["state"],
        "zip": address_info["zip"],
        
        # Additional fields that might be present in some listings
        "website": rec.get("website", ""),
        "email": rec.get("email", ""),
        "fax": rec.get("fax", ""),
        "description": rec.get("description", ""),
        "business_hours": rec.get("businessHours", ""),
        "hours": rec.get("hours", ""),
        "services": join_list(rec.get("services")),
        "specialties": join_list(rec.get("specialties")),
        "payment_methods": join_list(rec.get("paymentMethods")),
        "languages": join_list(rec.get("languages")),
        "years_in_business": rec.get("yearsInBusiness", ""),
        "employees": rec.get("employees", ""),
        "verified": rec.get("verified", ""),
        "claimed": rec.get("claimed", ""),
        
        # Geographic information
        "latitude": rec.get("latitude", "") or rec.get("lat", ""),
        "longitude": rec.get("longitude", "") or rec.get("lng", ""),
        
        # Social media
        "facebook": rec.get("facebook", ""),
        "twitter": rec.get("twitter", ""),
        "instagram": rec.get("instagram", ""),
        "linkedin": rec.get("linkedin", ""),
        
        # Additional images/media
        "logo": rec.get("logo", ""),
        "photos": join_list(rec.get("photos")),
        "menu_url": rec.get("menuUrl", ""),
        
        # Business status
        "is_closed": rec.get("isClosed", ""),
        "permanently_closed": rec.get("permanentlyClosed", ""),
        
        # Keep raw data for debugging and any additional fields
        "_raw": json.dumps(rec, ensure_ascii=False),
    }


def save_jsonl(path: str, rows: Iterable[Dict[str, Any]]) -> int:
    count = 0
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            count += 1
    return count


def save_csv(path: str, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        # write a header-only file with common columns
        fieldnames = list(normalize_record({}).keys())
        with open(path, "w", encoding="utf-8", newline="") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()
        return 0

    # Union of keys across all rows (keeps extras if actor adds fields)
    fieldnames = []
    seen = set()
    # Prefer normalized columns first
    preferred = list(normalize_record({}).keys())
    for k in preferred:
        if k not in seen:
            fieldnames.append(k)
            seen.add(k)
    # Add any other keys found
    for r in rows:
        for k in r.keys():
            if k not in seen:
                fieldnames.append(k)
                seen.add(k)

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Scrape Yellow Pages via Apify.")
    parser.add_argument("-c", "--category", required=True, help='Business category, e.g., "Plumber"')
    parser.add_argument("-l", "--location", required=True, help='U.S. state abbreviation, e.g., "NC", "CA", "TX"')
    parser.add_argument("-o", "--output-prefix", default=None, help="Output file prefix (default: yelp_<category>_<location>_<date>)")
    parser.add_argument("--debug", action="store_true", help="Enable actor debug mode.")
    parser.add_argument("--token", default=None, help="Apify API token (falls back to APIFY_TOKEN env var).")
    parser.add_argument(
        "--max-items",
        type=int,
        default=30,  # <-- default changed to 30
        help="Maximum items to fetch (0 for unlimited, default: 30).",
    )
    parser.add_argument(
        "--max-cost",
        type=float,
        default=5.0,
        help="Maximum cost per run in USD (default: 5.0).",
    )
    args = parser.parse_args()
    
    # Validate location is a U.S. state abbreviation
    location_upper = args.location.upper().strip()
    if location_upper not in US_STATES:
        print(f"ERROR: Location must be a valid U.S. state abbreviation. Got: '{args.location}'", file=sys.stderr)
        print(f"Valid options: {', '.join(sorted(US_STATES))}", file=sys.stderr)
        sys.exit(1)
    
    # Use the uppercase version
    args.location = location_upper
    
    # export APIFY_TOKEN="YOUR_APIFY_TOKEN"
    token = args.token or os.getenv("APIFY_TOKEN")
    if not token:
        print("ERROR: Provide Apify token via --token or APIFY_TOKEN environment variable.", file=sys.stderr)
        sys.exit(1)

    client = ApifyClient(token)

    try:
        run = run_yellow_pages_actor(
            client=client,
            category=args.category,
            location=args.location,
            max_items=args.max_items,
            debug=args.debug,
            max_cost=args.max_cost,
        )
    except Exception as e:
        print(f"Error when starting actor: {e}", file=sys.stderr)
        sys.exit(2)

    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        print("No dataset produced by the actor run.", file=sys.stderr)
        sys.exit(3)

    # Build output names
    safe_cat = args.category.strip().lower().replace(" ", "_")
    safe_loc = args.location.strip().lower().replace(" ", "_").replace(",", "")
    date_tag = datetime.now().strftime("%Y%m%d")
    prefix = args.output_prefix or f"yellowpages_{safe_cat}_{safe_loc}_{date_tag}"

    # Stream items, normalize, and buffer for CSV
    normalized = []
    raw_jsonl_path = f"{prefix}.jsonl"
    csv_path = f"{prefix}.csv"

    # First pass: write JSONL while collecting normalized rows
    print("Fetching results...")
    fetched = 0
    with open(raw_jsonl_path, "w", encoding="utf-8") as jf:
        for item in iter_results(client, dataset_id):
            jf.write(json.dumps(item, ensure_ascii=False) + "\n")
            normalized.append(normalize_record(item))
            fetched += 1
            if fetched % 100 == 0:
                print(f"  ... {fetched} items")
                # Save intermediate CSV every 100 items
                print(f"  ... saving intermediate results to {csv_path}")
                save_csv(csv_path, normalized)

    # Save final CSV with updated filename that includes line count
    final_csv_path = f"{prefix}_{fetched}lines.csv"
    save_csv(final_csv_path, normalized)

    print(f"Done. Items fetched: {fetched}")
    print(f"JSONL: {raw_jsonl_path}")
    print(f"CSV  : {final_csv_path}")


if __name__ == "__main__":
    main()
