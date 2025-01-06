
import os
import gzip
import ijson
import requests
from concurrent.futures import ProcessPoolExecutor, as_completed

# Constants
BATCH_SIZE = 100000          # Number of items to process in each batch
MAX_WORKERS = 4              # Number of parallel processes (adjust based on your CPU)
EIN_FILE = "matched_ny_eins.txt"  # File storing known relevant EINs
INDEX_URL = "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2025-01-01_anthem_index.json.gz"
INDEX_FILE = "anthem_index.json.gz"
OUTPUT_FILE = "ny_ppo_urls.txt"

def download_index_file(url=INDEX_URL, dest=INDEX_FILE):
    """
    Downloads the anthem_index.json.gz file from the specified URL.
    If the file already exists, it skips downloading.
    """
    if os.path.exists(dest):
        print(f"[INFO] {dest} already exists. Skipping download.")
        return
    print(f"[INFO] Downloading {url} to {dest}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(dest, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    print(f"[DONE] Downloaded {dest}.")

def load_known_eins(ein_file_path):
    """
    Loads a list of 'NY-relevant' EINs from disk if the file exists,
    so we can skip large blocks of irrelevant data.
    """
    known_eins = set()
    if os.path.exists(ein_file_path):
        with open(ein_file_path, "r", encoding="utf-8") as f_in:
            for line in f_in:
                stripped = line.strip()
                if stripped:
                    known_eins.add(stripped)
        print(f"[INFO] Loaded {len(known_eins)} known EIN(s) from {ein_file_path}")
    else:
        print(f"[INFO] No existing EIN file {ein_file_path}; starting fresh.")
    return known_eins

def write_known_eins(ein_file_path, all_eins):
    """
    Writes all known relevant EINs to disk (sorted for consistency).
    """
    with open(ein_file_path, "w", encoding="utf-8") as f_out:
        for ein in sorted(all_eins):
            f_out.write(ein + "\n")
    print(f"[INFO] Wrote {len(all_eins)} EIN(s) to {ein_file_path}")

def _desc_has_ny_ppo(desc):
    """
    Returns True if description suggests a New York PPO line.
    Checks for 'ppo' and 'new york' or 'ny' with word boundaries.
    """
    desc = desc.lower()
    if "ppo" not in desc:
        return False
    # Check for 'new york'
    if "new york" in desc:
        return True
    # Check for 'ny' with word boundaries
    ny_variants = ["ny ", "ny,", "ny-", "ny_", "nyc"]
    return any(variant in desc for variant in ny_variants)

def _process_chunk(chunk_of_items):
    """
    Processes a chunk of structure_item objects to find matching URLs.
    """
    matched_urls = []
    for structure_item in chunk_of_items:
        for fdict in structure_item.get("in_network_files", []):
            desc = fdict.get("description", "")
            if _desc_has_ny_ppo(desc):
                url = fdict.get("location")
                if url:
                    matched_urls.append(url)
    return matched_urls

def parse_anthem_ppo_ny_parallel(anthem_gz_path, output_path=OUTPUT_FILE):
    """
    Streams the large Anthem .gz JSON index with ijson, extracting relevant URLs.
    """
    # Load known EINs
    known_eins = load_known_eins(EIN_FILE)
    newly_matched_eins = set()

    # Prepare for parallel processing
    with open(output_path, "w", encoding="utf-8") as f_out,          ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:

        chunk_buffer = []
        futures = []
        total_matched = 0

        def _submit_chunk_if_any():
            if not chunk_buffer:
                return
            chunk_copy = chunk_buffer[:]
            chunk_buffer.clear()
            futures.append(executor.submit(_process_chunk, chunk_copy))

        # Stream and parse the .gz file
        with gzip.open(anthem_gz_path, 'rb') as f_in:
            parser = ijson.items(f_in, 'reporting_structure.item')
            for structure_item in parser:
                plans = structure_item.get("reporting_plans", [])
                relevant = False

                for plan in plans:
                    plan_id_type = plan.get("plan_id_type", "").lower()
                    plan_id = plan.get("plan_id", "")
                    plan_name = plan.get("plan_name", "").lower()

                    # Check if the plan is Anthem and relevant to NY PPO
                    if "anthem" in plan_name and "ppo" in plan_name:
                        if "ny" in plan_name or "new york" in plan_name:
                            relevant = True
                            if plan_id_type == "ein" and plan_id:
                                newly_matched_eins.add(plan_id)

                if relevant:
                    chunk_buffer.append(structure_item)
                    if len(chunk_buffer) >= BATCH_SIZE:
                        _submit_chunk_if_any()

        # Submit any remaining chunks
        _submit_chunk_if_any()

        # Collect results
        for future in as_completed(futures):
            matched_sublist = future.result()
            total_matched += len(matched_sublist)
            for url in matched_sublist:
                f_out.write(url + "\n")

    # Update known EINs
    if newly_matched_eins:
        known_eins.update(newly_matched_eins)
        write_known_eins(EIN_FILE, known_eins)
    else:
        print(f"[INFO] No new EINs discovered. {len(known_eins)} total remain in {EIN_FILE}.")

    print(f"[DONE] Found {total_matched} matching URL(s). Output -> {output_path}")

def main():
    # Step 1: Download the index file
    download_index_file()

    # Step 2: Parse the index file
    parse_anthem_ppo_ny_parallel(INDEX_FILE)

if __name__ == "__main__":
    main()
