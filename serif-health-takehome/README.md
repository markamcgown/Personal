
# Serif Health Takehome

## Description
This project extracts URLs for Anthem PPO plans in New York state from Anthem's Transparency in Coverage index file.

## Key Highlights
1. **Efficient Streaming:** Utilizes `ijson` to stream large JSON data without loading it entirely into memory.
2. **Parallel Processing:** Employs `ProcessPoolExecutor` to speed up the extraction process.
3. **Heuristic Matching:** Implements heuristics to accurately identify relevant plans and URLs.

## Setup

### Prerequisites
- **Python 3.8** or higher

### Installation
1. **Clone the Repository:**
    ```bash
    git clone https://github.com/markamcgown/Personal.git
    cd Personal/serif-health-takehome
    ```

2. **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1. **Run the Script:**
    ```bash
    python main.py
    ```

    This will perform the following:
    - **Download** the `anthem_index.json.gz` file if it doesn't already exist.
    - **Process** the downloaded file to extract URLs for Anthem PPO plans in New York state.
    - **Generate** two output files:
        - `ny_ppo_urls.txt`: List of URLs for Anthem PPO plans in New York state.
        - `matched_ny_eins.txt`: List of known relevant EINs for faster processing in future runs.

## Files

- `main.py`: Script to download and process the Anthem index file.
- `requirements.txt`: Lists Python dependencies (`ijson`, `requests`).
- `README.md`: Documentation for the project.
- `ny_ppo_urls.txt`: Output file containing the matching URLs.
- `matched_ny_eins.txt`: File storing known relevant EINs.

## Questions Addressed

### How do you handle large file sizes?
By streaming data using `ijson` and processing in chunks with `gzip`, we minimize memory usage and efficiently handle large files that exceed memory limitations.

### How are URLs structured?
URLs reflect state-based Anthem subdomains, such as:
- `anthembcbsga.mrf.bcbs.com` for Georgia
- `anthembcbsky.mrf.bcbs.com` for Kentucky

This indicates different regional domains handling various state data.

### Is the 'description' field helpful?
The `description` field is partially helpful but not entirely consistent. It is used alongside heuristics to improve reliability in identifying relevant URLs.

## Future Improvements
- **Comprehensive Testing:** Implement detailed tests to cover edge cases in the data.
- **Extended Support:** Add functionality to handle in-network MRFs for more comprehensive data extraction.
- **Enhanced Heuristics:** Improve the heuristic algorithms to reduce false positives and negatives.

## Runtime
- **Parsing and Processing Time:** Approximately 10–15 minutes, depending on system resources and file size.

## License
This project is licensed under the MIT License.
