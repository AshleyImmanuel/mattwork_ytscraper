"""
Excel Generator — Compiles scraped data into a downloadable .xlsx file.
"""
import os
import pandas as pd
from datetime import datetime

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def _env_csv(name: str, default_csv: str) -> list[str]:
    raw = os.getenv(name, default_csv)
    return [item.strip() for item in raw.split(",") if item.strip()]


# Desired column order matching the PRD output spec
COLUMNS = _env_csv(
    "EXCEL_OUTPUT_COLUMNS",
    "title,id,viewCount,likes,duration,date,channelName,url,channelUrl,numberOfSubscribers,EMAIL,Country"
)


def generate_excel(results: list[dict], keyword: str) -> str:
    """
    Takes a list of result dicts and writes them to an .xlsx file.
    Returns the absolute path to the generated file.
    """
    df = pd.DataFrame(results)

    # Ensure all expected columns exist (fill missing with empty)
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[COLUMNS]  # enforce column order

    # Build filename
    safe_keyword = "".join(c if c.isalnum() else "_" for c in keyword)[:30]
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"YTLeads_{safe_keyword}_{timestamp}.xlsx"
    filepath = os.path.join(OUTPUT_DIR, filename)

    # Write with openpyxl for .xlsx support
    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Leads")

        # Auto-fit column widths
        worksheet = writer.sheets["Leads"]
        for col_idx, col_name in enumerate(COLUMNS, 1):
            max_len = max(len(str(col_name)), df[col_name].astype(str).str.len().max())
            worksheet.column_dimensions[
                worksheet.cell(row=1, column=col_idx).column_letter
            ].width = min(max_len + 4, 50)

    return filepath
