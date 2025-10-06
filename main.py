from fastapi import FastAPI, HTTPException, Query
import requests
import pandas as pd
from io import StringIO
import numpy as np

app = FastAPI()

CSV_URL = "https://data.opensanctions.org/datasets/20250806/us_ofac_sdn/targets.simple.csv"

def load_sdn_data():
    try:
        response = requests.get(CSV_URL)
        response.raise_for_status()
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)

        # Clean dataframe: remove NaN and infinite values
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.fillna("")  # Replace NaNs with empty strings

        return df
    except Exception as e:
        print(f"Error loading SDN data: {e}")
        return pd.DataFrame()

@app.get("/ping")
async def ping():
    return {"status": "ok"}

@app.get("/getsdn")
async def get_sdn(
    name: str = Query(None, description="Name to search for (partial match)"),
    country: str = Query(None, description="Country code to filter by (e.g., 'ir' for Iran, 'kp' for North Korea)"),
    limit: int = Query(100, description="Max number of results"),
):
    df = load_sdn_data()

    if df.empty:
        raise HTTPException(status_code=500, detail="Unable to load SDN data.")

    matches = df

    # Filter by name (partial match, case-insensitive)
    if name:
        matches = matches[matches["name"].str.contains(name, case=False, na=False)]
    
    # Filter by country code (exact match on country codes like 'ir', 'kp', 'ru')
    if country:
        country_lower = country.lower()
        # Check if the country code appears in the countries field
        matches = matches[matches["countries"].str.lower().str.contains(country_lower, na=False)]
    
    # Apply limit
    matches = matches.head(limit)

    return matches.to_dict(orient="records")