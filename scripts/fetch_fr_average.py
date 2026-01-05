import os
import io
import json
from pathlib import Path

import pandas as pd
import requests

API_URL = "https://api.bitcoinmagazinepro.com/metrics/fr-average"
# If this ever breaks, try:
# API_URL = "https://api.bitcoinmagazinepro.com/v1/metrics/fr-average"


def main():
    api_key = os.environ["BMP_API_KEY"]

    headers = {"Authorization": f"Bearer {api_key}"}

    resp = requests.get(API_URL, headers=headers, timeout=30)
    print("BMP API status code:", resp.status_code)
    resp.raise_for_status()

    raw_text = resp.text
    if not raw_text.strip():
        raise RuntimeError("Empty response from BMP API")

    # BMP returns a quoted string with literal "\n"
    csv_quoted = raw_text.strip()
    if csv_quoted.startswith('"') and csv_quoted.endswith('"'):
        csv_quoted = csv_quoted[1:-1]
    csv_text = csv_quoted.replace("\\n", "\n")

    df = pd.read_csv(io.StringIO(csv_text))
    print("Parsed columns:", list(df.columns))

    if "Date" not in df.columns:
        raise RuntimeError(f"Expected 'Date' column, got {list(df.columns)}")

    # Find the funding-rate column (varies by endpoint naming)
    value_col = None
    for candidate in ["fr_average", "fr-average", "funding_rate", "FundingRate", "value", "Value"]:
        if candidate in df.columns:
            value_col = candidate
            break

    # fallback: pick the first numeric-looking column that isn't Date/Price
    if value_col is None:
        ignore = {"Date", "Price", "MarketCap", "marketcap", "timestamp"}
        for c in df.columns:
            if c in ignore:
                continue
            value_col = c
            break

    if value_col is None:
        raise RuntimeError(f"Could not find a value column. Columns: {list(df.columns)}")

    df = df.set_index("Date")
    series = df[value_col].dropna()

    data = [{"date": str(d), "value": float(v)} for d, v in zip(series.index, series.values)]

    out_path = Path("data/fr-average.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, indent=2))

    print(f"Wrote {len(data)} points to {out_path} using column '{value_col}'")


if __name__ == "__main__":
    main()
