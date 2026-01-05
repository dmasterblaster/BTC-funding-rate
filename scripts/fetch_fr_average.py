import os
import io
import json
from pathlib import Path

import pandas as pd
import requests

API_URL = "https://api.bitcoinmagazinepro.com/metrics/fr-average"
# If this ever breaks, try:
# API_URL = "https://api.bitcoinmagazinepro.com/v1/metrics/fr-average"


def _unquote_bmp_csv(raw_text: str) -> str:
    if not raw_text.strip():
        raise RuntimeError("Empty response from BMP API")

    csv_quoted = raw_text.strip()
    if csv_quoted.startswith('"') and csv_quoted.endswith('"'):
        csv_quoted = csv_quoted[1:-1]

    return csv_quoted.replace("\\n", "\n")


def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None


def main():
    api_key = os.environ["BMP_API_KEY"]
    headers = {"Authorization": f"Bearer {api_key}"}

    resp = requests.get(API_URL, headers=headers, timeout=30)
    print("BMP API status code:", resp.status_code)
    resp.raise_for_status()

    csv_text = _unquote_bmp_csv(resp.text)
    df = pd.read_csv(io.StringIO(csv_text))

    if df.empty:
        raise RuntimeError("Parsed empty DataFrame from CSV.")

    print("Parsed columns:", list(df.columns))

    # Common BMP shape includes a leading unnamed index column because of the first comma.
    # We do not need it.
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

    date_col = _pick_column(df, ["Date", "date", "Timestamp", "timestamp"])
    if not date_col:
        raise RuntimeError(f"Could not find Date column. Columns: {list(df.columns)}")

    price_col = _pick_column(df, ["Price", "price"])

    # Funding column can vary, so we try the common names first, then fall back to "the non-date non-price numeric column".
    fr_col = _pick_column(df, ["fr_average", "fr-average", "funding_rate", "FundingRate", "funding", "FR", "fr"])
    if not fr_col:
        # fallback: choose the last numeric column that is not date or price
        exclude = {date_col}
        if price_col:
            exclude.add(price_col)

        numeric_cols = []
        for c in df.columns:
            if c in exclude:
                continue
            # test numeric convertibility
            s = pd.to_numeric(df[c], errors="coerce")
            if s.notna().any():
                numeric_cols.append(c)

        if not numeric_cols:
            raise RuntimeError(
                f"Could not infer funding rate column. Columns: {list(df.columns)}"
            )

        # Prefer columns whose name contains "fr" or "fund"
        preferred = [c for c in numeric_cols if ("fr" in str(c).lower() or "fund" in str(c).lower())]
        fr_col = preferred[0] if preferred else numeric_cols[-1]

    print("Using date_col:", date_col)
    print("Using price_col:", price_col)
    print("Using fr_col:", fr_col)

    out = pd.DataFrame()
    out["date"] = df[date_col].astype(str)

    # Funding rate (as float). BMP often provides decimals like 0.00073 = 0.073%
    out["funding_rate"] = pd.to_numeric(df[fr_col], errors="coerce")

    # Price optional, but you said it exists, so we include if present
    if price_col:
        out["price"] = pd.to_numeric(df[price_col], errors="coerce")
    else:
        out["price"] = None

    out = out.dropna(subset=["date", "funding_rate"])
    out = out.drop_duplicates(subset=["date"]).sort_values("date")

    payload = out.to_dict(orient="records")

    out_path = Path("data/fr-average.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2))

    print(f"Wrote {len(payload)} points to {out_path}")


if __name__ == "__main__":
    main()
