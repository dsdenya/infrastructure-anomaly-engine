import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def create_master_dataset(ufo_df: pd.DataFrame, 
                          outage_df: pd.DataFrame, 
                          plant_df: pd.DataFrame, 
                          census_df: pd.DataFrame) -> pd.DataFrame:

    logger.info("Merging datasets...")

    if ufo_df.empty and outage_df.empty:
        logger.error("Both UFO and Outage datasets are empty. Cannot merge.")
        return pd.DataFrame()

    master_df = pd.merge(ufo_df, outage_df, on=['state', 'datetime'], how='outer')

    if not plant_df.empty:
        master_df = pd.merge(master_df, plant_df, on='state', how='left')
    
    if not census_df.empty:
        master_df = pd.merge(master_df, census_df, on='state', how='left')

    fill_cols = ['UFO_Count', 'Unknown_Outage_Count', 'Total_Capacity_MW']
    for col in fill_cols:
        if col in master_df.columns:
            master_df[col] = master_df[col].fillna(0)
    
    master_df = master_df.sort_values(by=['state', 'datetime'])

    if 'population' in master_df.columns:
        valid_rows = master_df['population'] > 0
        
        master_df['UFO_Rate_100k'] = 0.0
        master_df['Outage_Rate_100k'] = 0.0
        
        master_df.loc[valid_rows, 'UFO_Rate_100k'] = (
            master_df.loc[valid_rows, 'UFO_Count'] / master_df.loc[valid_rows, 'population']
        ) * 100_000
        
        master_df.loc[valid_rows, 'Outage_Rate_100k'] = (
            master_df.loc[valid_rows, 'Unknown_Outage_Count'] / master_df.loc[valid_rows, 'population']
        ) * 100_000
        
        logger.info("Feature engineering (population normalization) complete.")
    else:
        logger.warning("Population data missing. skipped normalization.")

    return master_df

def save_dataset(df: pd.DataFrame, filepath: str):
    try:
        df.to_csv(filepath, index=False)
        logger.info(f"SUCCESS: Master dataset saved to {filepath}")
    except Exception as e:
        logger.error(f"Failed to save file: {e}")

def print_correlation_report(df: pd.DataFrame):

    print("\n" + "="*40)
    print("   STATISTICAL ANALYSIS REPORT")
    print("="*40)
    
    required_cols = ['UFO_Rate_100k', 'Outage_Rate_100k', 'NUCLEAR_MW']
    
    if all(col in df.columns for col in required_cols):
        corr = df[required_cols].corr()
        
        ufo_outage = corr.loc['UFO_Rate_100k', 'Outage_Rate_100k']
        ufo_nuclear = corr.loc['UFO_Rate_100k', 'NUCLEAR_MW']
        
        print(f"Correlation: UFOs vs Mystery Outages (Normalized): {ufo_outage:.4f}")
        print(f"Correlation: UFOs vs Nuclear Capacity (Normalized):  {ufo_nuclear:.4f}")
        
        if abs(ufo_outage) < 0.1:
            print("\nCONCLUSION: No significant statistical relationship detected.")
        else:
            print("\nCONCLUSION: Potential relationship detected.")
    else:
        print("Insufficient data columns for correlation analysis.")
    print("="*40 + "\n")