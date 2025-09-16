#!/usr/bin/env python3
"""
Scrape Yellow Pages via Apify and export all results.

Single Category Usage:
  python scraper.py --category "Plumber" --location "NC" -o Plumber_NC_YellowPage --max-items 260
  python scraper.py -c "Dentist" -l "CA" -o dentists_ca --max-items 0
  python scraper.py -c "Restaurant" -l "tx" --max-items 50

Batch Processing Usage:
  python scraper.py --batch-file input1.txt --location "NC" --max-items 100
  python scraper.py --batch-file input1.txt --location "CA" --output-dir "california_scrape"

  Where input1.txt contains one category per line, e.g.:
    Plumber
    Electrician
    HVAC Contractor
    General Contractor

APIFY_TOKEN is read from the APIFY_TOKEN env var or --token.
Location must be a valid U.S. state abbreviation (e.g., NC, CA, TX, FL).
--max-items set to 0 means no limit (default: 30).
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

    # Create mutually exclusive group for single category vs batch processing
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-c", "--category", help='Business category, e.g., "Plumber"')
    group.add_argument("--batch-file", help='File containing list of categories to scrape (one per line)')

    parser.add_argument("-l", "--location", required=True, help='U.S. state abbreviation, e.g., "NC", "CA", "TX"')
    parser.add_argument("-o", "--output-prefix", default=None, help="Output file prefix (default: yelp_<category>_<location>_<date>)")
    parser.add_argument("--output-dir", default=None, help="Output directory for batch processing (default: creates timestamped directory)")
    parser.add_argument("--debug", action="store_true", help="Enable actor debug mode.")
    parser.add_argument("--token", default=None, help="Apify API token (falls back to APIFY_TOKEN env var).")
    parser.add_argument(
        "--max-items",
        type=int,
        default=30,  # <-- default changed to 30
        help="Maximum items to fetch (0 for unlimited, default: 30).",
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
    
    token = args.token or os.getenv("APIFY_TOKEN")
    if not token:
        print("ERROR: Provide Apify token via --token or APIFY_TOKEN environment variable.", file=sys.stderr)
        sys.exit(1)

    client = ApifyClient(token)

    # Handle batch processing vs single category
    if args.batch_file:
        process_batch(client, args)
    else:
        process_single_category(client, args, args.category)


def process_batch(client: ApifyClient, args):
    """Process multiple categories from a batch file."""
    # Create output directory
    date_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_loc = args.location.strip().lower().replace(" ", "_").replace(",", "")
    output_dir = args.output_dir or f"batch_scrape_{safe_loc}_{date_tag}"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    # Read categories from file
    try:
        with open(args.batch_file, 'r', encoding='utf-8') as f:
            categories = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading batch file {args.batch_file}: {e}", file=sys.stderr)
        sys.exit(1)

    if not categories:
        print("No categories found in batch file.", file=sys.stderr)
        sys.exit(1)

    print(f"Processing {len(categories)} categories from {args.batch_file}")
    print(f"Output directory: {output_dir}")

    successful = 0
    failed = 0

    for i, category in enumerate(categories, 1):
        print(f"\n=== Processing category {i}/{len(categories)}: {category} ===")

        try:
            # Create category-specific output paths
            safe_cat = category.strip().lower().replace(" ", "_")
            safe_loc = args.location.strip().lower().replace(" ", "_").replace(",", "")
            date_tag = datetime.now().strftime("%Y%m%d")
            prefix = os.path.join(output_dir, f"yellowpages_{safe_cat}_{safe_loc}_{date_tag}")

            # Process single category with custom prefix
            process_single_category_with_prefix(client, args, category, prefix)
            successful += 1
            print(f"✓ Successfully processed: {category}")

        except Exception as e:
            failed += 1
            print(f"✗ Failed to process {category}: {e}", file=sys.stderr)
            continue

    print(f"\n=== Batch processing complete ===")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Output directory: {output_dir}")


def process_single_category(client: ApifyClient, args, category: str):
    """Process a single category with default output naming."""
    # Build output names
    safe_cat = category.strip().lower().replace(" ", "_")
    safe_loc = args.location.strip().lower().replace(" ", "_").replace(",", "")
    date_tag = datetime.now().strftime("%Y%m%d")
    prefix = args.output_prefix or f"yellowpages_{safe_cat}_{safe_loc}_{date_tag}"

    process_single_category_with_prefix(client, args, category, prefix)


def process_single_category_with_prefix(client: ApifyClient, args, category: str, prefix: str):
    """Process a single category with a specific output prefix."""
    try:
        run = run_yellow_pages_actor(
            client=client,
            category=category,
            location=args.location,
            max_items=args.max_items,
            debug=args.debug,
        )
    except Exception as e:
        print(f"Error when starting actor for {category}: {e}", file=sys.stderr)
        raise

    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        print(f"No dataset produced by the actor run for {category}.", file=sys.stderr)
        raise Exception("No dataset produced")

    # Stream items, normalize, and buffer for CSV
    normalized = []
    raw_jsonl_path = f"{prefix}.jsonl"
    csv_path = f"{prefix}.csv"

    # First pass: write JSONL while collecting normalized rows
    print(f"Fetching results for {category}...")
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
    final_csv_path = f"{prefix}_{fetched}_lines.csv"
    save_csv(final_csv_path, normalized)

    print(f"Done. Items fetched for {category}: {fetched}")
    print(f"JSONL: {raw_jsonl_path}")
    print(f"CSV  : {final_csv_path}")


if __name__ == "__main__":
    main()
