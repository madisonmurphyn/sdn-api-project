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

@app.get("/getsdn")
async def get_sdn(name: str = Query(..., description="Exact name to search for")):
    df = load_sdn_data()

    if df.empty:
        raise HTTPException(status_code=500, detail="Unable to load SDN data.")

    # Exact match filter (case-insensitive)
    matches = df[df["name"].str.strip().str.lower() == name.strip().lower()]

    if matches.empty:
        raise HTTPException(status_code=404, detail=f"No exact matches found for '{name}'.")

    return matches.to_dict(orient="records")
