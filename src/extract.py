import os
import pandas as pd 
import glob
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def read_file(filepath:str,encoding='utf-8', dtype=None) -> pd.DataFrame:
    try:
        return pd.read_csv(filepath,encoding=encoding,dtype=dtype)
    except FileNotFoundError:
        logger.error(f"CRITICAL: File not found at {filepath}")
        return pd.DataFrame()
    except Exception as e: 
        logger.error(f"Error reading {filepath}:{e}")
        return pd.DataFrame()

def ingest_ufo_data(filepath: str) -> pd.DataFrame:
    return read_file(filepath, dtype='unicode')

def ingest_plant_data(filepath: str) -> pd.DataFrame:
    return read_file(filepath, encoding='ISO-8859-1')

def ingest_census_data(filepath: str) -> pd.DataFrame:
    return read_file(filepath, dtype='unicode')

def ingest_outage_data(raw_pattern: str, combined_output_path: str) -> pd.DataFrame:

    if not os.path.exists(combined_output_path):
        logger.info(f"Combined file '{combined_output_path}' not found.")
        logger.info("Building it from raw 'eaglei' files...")
        
        all_files = glob.glob(raw_pattern)
        
        if not all_files:
            logger.warning(f"No files found matching pattern: {raw_pattern}")
            return pd.DataFrame()

        dfs = []
        for filename in all_files:
            if combined_output_path not in filename:
                try:
                    df = pd.read_csv(filename)
                    dfs.append(df)
                except Exception as e:
                    logger.warning(f"Skipping bad file {filename}: {e}")

        if dfs:
            final_df = pd.concat(dfs, ignore_index=True)
            final_df.to_csv(combined_output_path, index=False)
            logger.info(f"Successfully created '{combined_output_path}'")
        else:
            logger.error("No valid outage files could be read.")
            return pd.DataFrame()

    else:
        logger.info(f"Found existing outage file: {combined_output_path}")

    return read_file(combined_output_path, dtype='unicode')