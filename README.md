# Sedgwick County Live Delinquent-Tax Collector

This project starts with the official 2024 publication CSV, deduplicates owner names, enters each owner into the Sedgwick County delinquent-tax search, saves the live rows, and matches them back to the published parcel list.

## Important limitation

The collector has been syntax-tested, but this execution environment could not directly run the live browser against the county site. The first 20-owner headed test is intentionally included to confirm the current page controls and returned-table headings. Any failed page is saved under `output/debug/` as HTML and a screenshot.

## Files

- `collector.py` — resumable Playwright collector
- `requirements.txt` — Python dependencies
- `run_test_windows.bat` — installs dependencies and runs 20 owners visibly
- `run_full_windows.bat` — resumes the full list after the test

Place `Sedgwick_County_2024_Delinquent_Real_Estate_Raw.csv` in this folder before running.

## Windows first test

Double-click `run_test_windows.bat`, or run:

```powershell
py -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
python collector.py --input Sedgwick_County_2024_Delinquent_Real_Estate_Raw.csv --output-dir output --limit 20 --headed
```

Review:

- `output/live_search_results.csv`
- `output/matched_2024_parcels.csv`
- `output/owner_search_status.csv`
- `output/debug/` for failures

Then run the full collector:

```powershell
python collector.py --input Sedgwick_County_2024_Delinquent_Real_Estate_Raw.csv --output-dir output --retry-errors
```

## Resume behavior

Progress is stored in `output/sedgwick_delinquent.sqlite3`. Restarting the same command skips owners already marked `found` or `not_found`. Add `--retry-errors` to retry only pending and failed owners.

## Matching logic

The collector attempts to match the live result back to the publication in this order:

1. Tax account
2. Parcel ID
3. Property address
4. Owner-only result flagged for review

Do not treat an owner-name-only match as definitive.

## Responsible request rate

The default delay is randomized between 2.5 and 5.5 seconds per unique owner. Do not remove the delay or run many concurrent browsers. The script is deliberately sequential.

## Next enrichment phase

Once live delinquency is confirmed, use tax account or parcel ID to obtain current appraisal, property class, mailing address, and billing detail from the county property-tax/appraisal application. Estimated equity requires a separate mortgage/lien data source; appraisal minus taxes alone is not true equity.
