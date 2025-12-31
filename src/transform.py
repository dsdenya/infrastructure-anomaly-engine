import os
import pandas as pd 
import logging
from src.config import US_STATE_TO_ABBREV, VALID_CODES, UNKNOWN_CAUSES_LIST

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def clean_ufo_data(df: pd.DataFrame) -> pd.DataFrame:

    if df.empty:
        logger.warning("UFO DataFrame is empty. Skipping transformation.")
        return df

    df = df[['datetime', 'state', 'shape', 'duration (seconds)']].copy()

    df['state'] = df['state'].str.upper()
    
    initial_count = len(df)
    df = df[df['state'].isin(VALID_CODES)]
    logger.info(f"Filtered UFO data: {initial_count} -> {len(df)} rows (removed non-US entries)")

    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df = df.dropna(subset=['datetime'])
    df['datetime'] = df['datetime'].dt.strftime('%Y-%m')

    df.rename(columns={'duration (seconds)': 'duration'}, inplace=True)
    df['duration'] = pd.to_numeric(df['duration'], errors='coerce')

    df = df.drop_duplicates()
    grouped = df.groupby(['datetime', 'state']).agg(
        UFO_Count=('shape', 'count'),
        Avg_Duration=('duration', 'mean')
    ).reset_index()

    return grouped


def clean_outage_data(df: pd.DataFrame) -> pd.DataFrame:

    if df.empty:
        logger.warning("Outage DataFrame is empty. Skipping transformation.")
        return df

    df = df[['Datetime Event Began', 'Event Type', 'state', 'duration']].copy()

    df['state'] = df['state'].astype(str).replace(US_STATE_TO_ABBREV)
    df = df[df['state'].isin(VALID_CODES)]

    df['datetime'] = pd.to_datetime(df['Datetime Event Began'], errors='coerce')
    df['datetime'] = df['datetime'].dt.strftime('%Y-%m')

    df['duration'] = pd.to_numeric(df['duration'], errors='coerce')

    mask = df['Event Type'].astype(str).apply(
        lambda x: any(cause in x for cause in UNKNOWN_CAUSES_LIST)
    )
    df = df[mask]
    logger.info(f"Identified {len(df)} suspicious outage events.")

    df = df.drop_duplicates()
    grouped = df.groupby(['datetime', 'state']).agg(
        Unknown_Outage_Count=('Event Type', 'count'),
        Total_Outage_Duration=('duration', 'sum')
    ).reset_index()

    return grouped


def clean_plant_data(df: pd.DataFrame) -> pd.DataFrame:

    if df.empty: return df

    df.columns = df.columns.str.lower()
    
    if 'plant state abbreviation' in df.columns:
        df.rename(columns={"plant state abbreviation": "state"}, inplace=True)
    
    if 'state' not in df.columns:
        logging.error(f"KEY ERROR: Could not find 'state' column in Plant Data. Found: {df.columns.tolist()}")
        return pd.DataFrame()

    fuel_col = 'plant primary fuel category'
    cap_col = 'plant nameplate capacity (mw)'
    
    if fuel_col not in df.columns or cap_col not in df.columns:
         logging.error("KEY ERROR: Missing fuel or capacity columns.")
         return pd.DataFrame()

    fuel_breakdown = df.groupby(['state', fuel_col])[cap_col].sum().unstack(fill_value=0)
    fuel_breakdown = fuel_breakdown.add_suffix('_MW')

    state_totals = df.groupby(['state']).agg(
        Total_Capacity_MW=(cap_col, 'sum')
    )

    final_df = pd.merge(state_totals, fuel_breakdown, on='state', how='left').reset_index()
    return final_df

def clean_census_data(df: pd.DataFrame) -> pd.DataFrame:

    if df.empty: return df

    try:
        df = df.iloc[0:1] 
        df = df.T         
        df.reset_index(inplace=True) 
        

        if df.shape[1] >= 2:
            df.columns.values[0] = 'state'
            df.columns.values[1] = 'population'
        else:
            logging.error(f"Census data shape is weird: {df.shape}")
            return pd.DataFrame()

        df['population'] = df['population'].astype(str).str.replace(',', '', regex=False)
        df['population'] = pd.to_numeric(df['population'], errors='coerce')
        df['state'] = df['state'].map(US_STATE_TO_ABBREV)
        
        return df.dropna(subset=['state'])

    except Exception as e:
        logging.error(f"Census cleaning failed: {e}")
        return pd.DataFrame()