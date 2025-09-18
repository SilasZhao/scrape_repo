#!/usr/bin/env python3
"""
Scrape Yellow Pages via Apify and export all results with multi-threading support.

Single Category Usage:
  python scraper.py --category "Plumber" --location "NC" -o Plumber_NC_YellowPage --max-items 260
  python scraper.py -c "Dentist" -l "Charlotte, NC" -o dentists_charlotte --max-items 100
  python scraper.py -c "Restaurant" -l "Los Angeles, CA" --max-items 50

Batch Processing Usage:
  # Single location, multiple categories (multi-threaded)
  python scraper.py --batch-file categories.txt --location "Charlotte, NC" --max-items 100 --max-threads 4

  # Multiple locations and categories (cross-product, multi-threaded)
  python scraper.py --batch-categories categories.txt --batch-locations locations.txt --max-items 50 --max-threads 8

  Where categories.txt contains one category per line:
    Plumber
    Electrician
    HVAC Contractor
    General Contractor

  And locations.txt contains one location per line:
    Charlotte, NC
    Raleigh, NC
    Durham, NC
    Greensboro, NC

Location Examples:
  - "NC" (state only)
  - "Charlotte, NC" (city and state)
  - "Los Angeles, CA"
  - "Austin, TX"
  - "Miami, FL"

Multi-threading:
  - Use --max-threads to control parallelism (default: 4)
  - Higher thread counts speed up batch processing
  - Each thread creates its own Apify client for thread safety

APIFY_TOKEN is read from the APIFY_TOKEN env var or --token.
--max-items set to 0 means no limit (default: 30).
Output: Only CSV files are generated (no JSON).
"""

import argparse
import csv
import json
import os
import sys
import re
from datetime import datetime
from typing import Dict, Any, Iterable, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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


def parse_location(location: str) -> Dict[str, str]:
    """Parse location string into city, state, and optional zip code components."""
    # Clean up the location string
    location = location.strip()

    # Support different formats:
    # "Charlotte, NC"
    # "Charlotte, NC 28202"
    # "Los Angeles, CA"
    # "NC" (state only - backwards compatibility)

    if ',' in location:
        # City, State [optional zip] format
        parts = [part.strip() for part in location.split(',')]
        if len(parts) == 2:
            city = parts[0]
            state_part = parts[1].strip()

            # Check if the state part contains a zip code
            # Pattern: "NC 28202" or "CA 90210"
            zip_pattern = re.compile(r'^([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$', re.IGNORECASE)
            zip_match = zip_pattern.match(state_part)

            if zip_match:
                # Found zip code
                state = zip_match.group(1).upper()
                zip_code = zip_match.group(2)
                full_location = f"{city}, {state} {zip_code}"
                safe_name = f"{city.lower().replace(' ', '_')}_{state.lower()}_{zip_code}"
            else:
                # No zip code, just state
                state = state_part.upper()
                zip_code = ''
                full_location = f"{city}, {state}"
                safe_name = f"{city.lower().replace(' ', '_')}_{state.lower()}"

            # Validate state abbreviation
            if state not in US_STATES:
                raise ValueError(f"Invalid state abbreviation: '{state}'. Must be one of: {', '.join(sorted(US_STATES))}")

            return {
                'city': city,
                'state': state,
                'zip_code': zip_code,
                'full_location': full_location,
                'safe_name': safe_name
            }
        else:
            raise ValueError(f"Invalid location format: '{location}'. Use 'City, State' or 'City, State Zip' format (e.g., 'Charlotte, NC' or 'Charlotte, NC 28202')")
    else:
        # State only (backwards compatibility)
        state_upper = location.upper()
        if state_upper not in US_STATES:
            raise ValueError(f"Invalid state abbreviation: '{location}'. Must be one of: {', '.join(sorted(US_STATES))}")

        return {
            'city': '',
            'state': state_upper,
            'zip_code': '',
            'full_location': state_upper,
            'safe_name': state_upper.lower()
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
    group.add_argument("--batch-categories", help='File containing list of categories (for cross-product with --batch-locations)')

    parser.add_argument("--batch-locations", help='File containing list of locations (used with --batch-categories for cross-product processing)')

    parser.add_argument("-l", "--location", required=False,
                       help='Location as "City, State" (e.g., "Charlotte, NC"), "City, State Zip" (e.g., "Charlotte, NC 28202"), or just state abbreviation (e.g., "NC"). Required for single category or --batch-file mode.')
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
    parser.add_argument(
        "--max-threads",
        type=int,
        default=4,
        help="Maximum number of threads for batch processing (default: 4).",
    )
    args = parser.parse_args()

    # Validate argument combinations
    if args.batch_categories and not args.batch_locations:
        print("ERROR: --batch-categories requires --batch-locations", file=sys.stderr)
        sys.exit(1)
    if args.batch_locations and not args.batch_categories:
        print("ERROR: --batch-locations requires --batch-categories", file=sys.stderr)
        sys.exit(1)
    if (args.category or args.batch_file) and not args.location:
        print("ERROR: --location is required for single category or --batch-file mode", file=sys.stderr)
        sys.exit(1)

    # Parse and validate location (only for non-cross-product modes)
    location_info = None
    if args.location:
        try:
            location_info = parse_location(args.location)
            print(f"Scraping location: {location_info['full_location']}")
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)
    

    token = args.token or os.getenv("APIFY_TOKEN")
    if not token:
        print("ERROR: Provide Apify token via --token or APIFY_TOKEN environment variable.", file=sys.stderr)
        sys.exit(1)

    client = ApifyClient(token)

    # Handle different processing modes
    if args.batch_categories and args.batch_locations:
        process_cross_product_batch(client, args)
    elif args.batch_file:
        process_batch(client, args, location_info)
    else:
        process_single_category(client, args, args.category, location_info)


def process_single_job(args, category: str, location_info: Dict[str, str], output_dir: str, job_num: int, total_jobs: int, token: str) -> Dict[str, Any]:
    """Process a single category-location combination in a thread-safe manner."""
    # Create a new client for this thread to avoid sharing issues
    client = ApifyClient(token)

    try:
        # Create combination-specific output paths
        safe_cat = category.strip().lower().replace(" ", "_")
        safe_loc = location_info['safe_name']
        date_tag = datetime.now().strftime("%Y%m%d")
        prefix = os.path.join(output_dir, f"yellowpages_{safe_cat}_{safe_loc}_{date_tag}")

        # Thread-safe printing with lock
        with threading.Lock():
            print(f"[{job_num}/{total_jobs}] Processing: {category} in {location_info['full_location']}")

        # Process single category-location combination
        process_single_category_with_prefix(client, args, category, location_info, prefix)

        return {
            'success': True,
            'category': category,
            'location': location_info['full_location'],
            'job_num': job_num
        }

    except Exception as e:
        return {
            'success': False,
            'category': category,
            'location': location_info['full_location'],
            'error': str(e),
            'job_num': job_num
        }


def process_cross_product_batch(_: ApifyClient, args):
    """Process cross-product of categories and locations from separate files using multi-threading."""
    # Read categories from file
    try:
        with open(args.batch_categories, 'r', encoding='utf-8') as f:
            categories = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading categories file {args.batch_categories}: {e}", file=sys.stderr)
        sys.exit(1)

    # Read locations from file
    try:
        with open(args.batch_locations, 'r', encoding='utf-8') as f:
            locations = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading locations file {args.batch_locations}: {e}", file=sys.stderr)
        sys.exit(1)

    if not categories:
        print("No categories found in categories file.", file=sys.stderr)
        sys.exit(1)
    if not locations:
        print("No locations found in locations file.", file=sys.stderr)
        sys.exit(1)

    # Validate all locations before starting
    parsed_locations = []
    for location in locations:
        try:
            location_info = parse_location(location)
            parsed_locations.append(location_info)
        except ValueError as e:
            print(f"ERROR: Invalid location '{location}': {e}", file=sys.stderr)
            sys.exit(1)

    # Create output directory
    date_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir or f"cross_product_scrape_{date_tag}"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    # Create job list for all combinations
    jobs = []
    job_num = 0
    for category in categories:
        for location_info in parsed_locations:
            job_num += 1
            jobs.append((category, location_info, job_num))

    total_jobs = len(jobs)
    print(f"Processing {len(categories)} categories × {len(parsed_locations)} locations = {total_jobs} combinations")
    print(f"Using {args.max_threads} threads")
    print(f"Output directory: {output_dir}")

    successful = 0
    failed = 0

    # Get token for thread workers
    token = args.token or os.getenv("APIFY_TOKEN")

    # Process jobs using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=args.max_threads) as executor:
        # Submit all jobs
        future_to_job = {
            executor.submit(process_single_job, args, category, location_info, output_dir, job_num, total_jobs, token):
            (category, location_info, job_num)
            for category, location_info, job_num in jobs
        }

        # Process completed jobs
        for future in as_completed(future_to_job):
            result = future.result()

            if result['success']:
                successful += 1
                print(f"✓ [{result['job_num']}/{total_jobs}] Successfully processed: {result['category']} in {result['location']}")
            else:
                failed += 1
                print(f"✗ [{result['job_num']}/{total_jobs}] Failed to process {result['category']} in {result['location']}: {result['error']}", file=sys.stderr)

    print(f"\n=== Cross-product batch processing complete ===")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Output directory: {output_dir}")


def process_batch(_: ApifyClient, args, location_info: Dict[str, str]):
    """Process multiple categories from a batch file using multi-threading."""
    # Create output directory
    date_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_loc = location_info['safe_name']
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
    print(f"Using {args.max_threads} threads")
    print(f"Output directory: {output_dir}")

    successful = 0
    failed = 0

    # Get token for thread workers
    token = args.token or os.getenv("APIFY_TOKEN")

    # Process categories using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=args.max_threads) as executor:
        # Submit all jobs
        future_to_job = {
            executor.submit(process_single_job, args, category, location_info, output_dir, i, len(categories), token):
            (category, i)
            for i, category in enumerate(categories, 1)
        }

        # Process completed jobs
        for future in as_completed(future_to_job):
            result = future.result()

            if result['success']:
                successful += 1
                print(f"✓ [{result['job_num']}/{len(categories)}] Successfully processed: {result['category']}")
            else:
                failed += 1
                print(f"✗ [{result['job_num']}/{len(categories)}] Failed to process {result['category']}: {result['error']}", file=sys.stderr)

    print(f"\n=== Batch processing complete ===")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Output directory: {output_dir}")


def process_single_category(client: ApifyClient, args, category: str, location_info: Dict[str, str]):
    """Process a single category with default output naming."""
    # Build output names
    safe_cat = category.strip().lower().replace(" ", "_")
    safe_loc = location_info['safe_name']
    date_tag = datetime.now().strftime("%Y%m%d")
    prefix = args.output_prefix or f"yellowpages_{safe_cat}_{safe_loc}_{date_tag}"

    process_single_category_with_prefix(client, args, category, location_info, prefix)


def process_single_category_with_prefix(client: ApifyClient, args, category: str, location_info: Dict[str, str], prefix: str):
    """Process a single category with a specific output prefix."""
    try:
        run = run_yellow_pages_actor(
            client=client,
            category=category,
            location=location_info['full_location'],  # Use full "City, State" or "State" format
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
    csv_path = f"{prefix}.csv"

    # Fetch and normalize all items
    print(f"Fetching results for {category} in {location_info['full_location']}...")
    fetched = 0
    for item in iter_results(client, dataset_id):
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

    print(f"Done. Items fetched for {category} in {location_info['full_location']}: {fetched}")
    print(f"CSV  : {final_csv_path}")


if __name__ == "__main__":
    main()
