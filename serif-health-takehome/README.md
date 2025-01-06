
# Serif Health Takehome

## Description
This project extracts URLs for Anthem PPO plans in New York state from Anthem's Transparency in Coverage index file.

## Key Highlights
1. **Efficient Streaming:** Utilizes \`ijson\` to stream large JSON data without loading it entirely into memory.
2. **Parallel Processing:** Employs \`ProcessPoolExecutor\` to speed up the extraction process.
3. **Heuristic Matching:** Implements heuristics to accurately identify relevant plans and URLs.

## Setup

### Prerequisites
- **Python 3.8** or higher

### Installation
1. **Clone the Repository:**
    \`\`\`bash
    git clone https://github.com/markamcgown/Personal.git
    cd Personal/serif-health-takehome
    \`\`\`

2. **Install Dependencies:**
    \`\`\`bash
    pip install -r requirements.txt
    \`\`\`

## Usage

1. **Run the Script:**
    \`\`\`bash
    python main.py
    \`\`\`

    This will perform the following:
    - **Download** the \`anthem_index.json.gz\` file if it doesn't already exist.
    - **Process** the downloaded file to extract URLs for Anthem PPO plans in New York state.
    - **Generate** two output files:
        - \`ny_ppo_urls.txt\`: List of URLs for Anthem PPO plans in New York state.
        - \`matched_ny_eins.txt\`: List of known relevant EINs for faster processing in future runs.

## Files

- \`main.py\`: Script to download and process the Anthem index file.
- \`requirements.txt\`: Lists Python dependencies (\`ijson\`, \`requests\`).
- \`README.md\`: Documentation for the project.
- \`ny_ppo_urls.txt\`: Output file containing the matching URLs.
- \`matched_ny_eins.txt\`: File storing known relevant EINs.

## Questions Addressed

### How do you handle the file size and format efficiently, when the uncompressed file will exceed memory limitations on most systems?

I avoid fully decompressing and loading the entire file at once by streaming it with libraries like \`ijson\` or Python’s \`gzip\` in chunked/batched increments. This way, each batch of objects is processed on-the-fly and discarded as soon as we extract relevant data. In addition, parallel processing (e.g., via \`ProcessPoolExecutor\`) allows me to speed up scanning without saturating a single core, and controlled chunk sizes (\`BATCH_SIZE\`) ensure I never exceed feasible memory limits, since only the current chunk plus worker overhead need to be stored at any point.

### When you look at your output URL list, which segments of the URL are changing, which segments are repeating, and what might that mean?

I see variations like \`anthembcbsga.mrf.bcbs.com\`, \`anthembcca.mrf.bcbs.com\`, and \`anthembcbsky.mrf.bcbs.com\`. This suggests that different state or regional domains exist (e.g., “bcbsga” might indicate Georgia, “bcca” might indicate California, “bcbsky” might indicate Kentucky, and so on).

- They’re systematically building paths for each plan’s in-network-rates file (and splitting large files into multiple parts).
- They distribute them across different state subdomains.
- They rely on S3 (or a similar storage) presigned links with expiration times.

### Is the 'description' field helpful? Is it complete? Does it change relative to 'location'? Is Highmark the same as Anthem?

- **Description:** Very useful for identifying the brand or network. But it’s not guaranteed to be 100% consistent or always brand-specific; some lines just say “In-Network Negotiated Rates Files.”
- **Completeness:** Not fully complete or uniform—some descriptions are brand + product, others are generic.
- **Relative to 'location':** The same or similar description may apply to multiple chunked files, so it doesn’t always uniquely map.
- **Highmark vs. Anthem:** They are distinct BCBS companies. They appear together in the data because of multi-state or reciprocal BCBS coverage, but they are separate licensees under the BCBS umbrella.

### Anthem has an interactive MRF lookup system. This lookup can be used to gather additional information - but it requires you to input the EIN or name of an employer who offers an Anthem health plan: Anthem EIN lookup. How might you find a business likely to be in the Anthem NY PPO? How can you use this tool to confirm if your answer is complete?

Essentially, the Anthem EIN lookup is a way to:
- **Identify** the exact files an employer group has, including those that might be chunked or named differently than expected.
- **Verify** you’ve captured all the relevant “NY PPO” references that Anthem publishes.
- **Confirm** your code’s results (the set of .json.gz links) against the official list. If they match up (or differ in a predictable way, e.g., out-of-network or out-of-area attachments), you can be confident that your code’s coverage of the NY PPO portion is complete.

## Sample Raw JSON Data

Imagine you have two \`structure_item\` objects in the large array:

### structure_item #1 (skippable)

\`\`\`json
{
  "reporting_plans": [
    {
      "plan_name": "KEY CARE - GA SPONSORS ONLY - ANTHEM",
      "plan_id_type": "EIN",
      "plan_id": "123456789",
      "plan_market_type": "group"
    }
    // (etc. - no mention of "NY" or "PPO")
  ],
  "in_network_files": [
    {
      "description": "BCBS Tennessee, Inc. : Network P",
      "location": "https://anthembcbsga.mrf.bcbs.com/2025-01_890_58B0_in-network-rates_23_of_120.json.gz"
    },
    {
      "description": "BCBS Alabama : Preferred Care",
      "location": "https://anthembcbsga.mrf.bcbs.com/2025-01_510_01B0_in-network-rates_21_of_29.json.gz"
    },
    {
      "description": "In-Network Negotiated Rates Files",
      "location": "https://anthembcbsga.mrf.bcbs.com/2025-01_131_17B0_in-network-rates_3_of_20.json.gz"
    }
    // ... potentially dozens more ...
  ]
}
\`\`\`

- **None of the \`plan_names\` mention New York or PPO.**
- **The EIN (123456789) might not be in your known EIN list** (assuming \`matched_ny_eins.txt\` is empty or does not list 123456789).
- **Result:** The new approach says “This entire structure_item is irrelevant,” so it skips the (potentially large) \`in_network_files\`.

### structure_item #2 (relevant)

\`\`\`json
{
  "reporting_plans": [
    {
      "plan_name": "ANTHEM NEW YORK PPO - EMPLOYER GROUP X",
      "plan_id_type": "EIN",
      "plan_id": "541495390",
      "plan_market_type": "group"
    },
    {
      "plan_name": "LIVE HEALTH ONLINE - (NY)",
      "plan_id_type": "EIN",
      "plan_id": "541495390",
      "plan_market_type": "group"
    }
    // ... possibly others ...
  ],
  "in_network_files": [
    {
      "description": "BCBS Tennessee, Inc. : Network P",
      "location": "https://anthembcbsva.mrf.bcbs.com/2025-01_890_58B0_in-network-rates_23_of_120.json.gz"
    },
    {
      "description": "BCBS Alabama : Preferred Care",
      "location": "https://anthembcbsva.mrf.bcbs.com/2025-01_510_01B0_in-network-rates_21_of_29.json.gz"
    },
    {
      "description": "Excellus BCBS : BluePPO",
      "location": "https://anthembcbsva.mrf.bcbs.com/2025-01_302_42B0_in-network-rates_3_of_3.json.gz"
    },
    // etc. (some might have "new york" in desc, some might not)
  ]
}
\`\`\`

- **The first plan’s \`plan_name\` has “ANTHEM NEW YORK PPO” → definitely relevant.**
- **If your known EINs file is empty, you still see “new york” + “ppo” in the plan name → relevant.**
- **Because that plan is relevant, the code passes this entire structure item to \`_process_chunk()\`.**
- **\`_process_chunk()\` then looks for \`in_network_files\` whose \`description\` includes “new york” + “ppo”. (Whether or not they truly do is up to the data. You might find none or multiple.)**

## Future Improvements
- **Comprehensive Testing:** Implement detailed tests to cover edge cases in the data.
- **Extended Support:** Add functionality to handle in-network MRFs for more comprehensive data extraction.
- **Enhanced Heuristics:** Improve the heuristic algorithms to reduce false positives and negatives.

## Runtime
- **Parsing and Processing Time:** Approximately 10–15 minutes, depending on system resources and file size.

## License
This project is licensed under the MIT License.
