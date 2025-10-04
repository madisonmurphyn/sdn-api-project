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
    name: str = Query(None, description="Name to search for"),
    country: str = Query(None, description="Country code to filter by"),
    limit: int = Query(100, description="Max number of results"),
    getsdn: str = Query(None, description="Use 'ALL' to return all SDN data"),
):
    df = load_sdn_data()

    if df.empty:
        raise HTTPException(status_code=500, detail="Unable to load SDN data.")

    # Start with all data
    if getsdn and getsdn.strip().upper() == "ALL":
        matches = df
    elif name:
        # Partial match instead of exact match
        matches = df[df["name"].str.contains(name, case=False, na=False)]
    else:
        matches = df  # Return all if no name specified
    
    # Filter by country if provided
    if country and not matches.empty:
        matches = matches[matches["countries"].str.contains(country, case=False, na=False)]
    
    # Apply limit
    if not matches.empty:
        matches = matches.head(limit)

    if matches.empty:
        return []  # Return empty array instead of error

    return matches.to_dict(orient="records")