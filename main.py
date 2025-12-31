import argparse
import logging
import os
from src import extract, transform, load


logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

def parse_arguments():

    parser = argparse.ArgumentParser(
        description="Infrastructure Anomaly Detection Engine: Correlates UFO sightings with Grid Outages."
    )
    
    parser.add_argument('--ufo', type=str, default='data/ufo.csv', help='Path to UFO CSV')
    parser.add_argument('--plants', type=str, default='data/powerplant.csv', help='Path to Power Plant CSV')
    parser.add_argument('--census', type=str, default='data/pop_census.csv', help='Path to Census CSV')
    parser.add_argument('--outage_dir', type=str, default='data/', help='Directory containing eaglei outage files')
    
    parser.add_argument('--output', type=str, default='Final_Report.csv', help='Path to save the final normalized dataset')
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    logger.info("--- STARTING PIPELINE ---")
    
    logger.info("PHASE 1: Extracting data from files...")
    
    ufo_raw = extract.ingest_ufo_data(args.ufo)
    plant_raw = extract.ingest_plant_data(args.plants)
    census_raw = extract.ingest_census_data(args.census)
    
    outage_pattern = os.path.join(args.outage_dir, "*eaglei*")
    combined_outage_file = os.path.join(args.outage_dir, "electricgrid.csv")
    outage_raw = extract.ingest_outage_data(outage_pattern, combined_outage_file)


    logger.info("PHASE 2: Transforming and cleaning data...")
    
    ufo_clean = transform.clean_ufo_data(ufo_raw)
    plant_clean = transform.clean_plant_data(plant_raw)
    census_clean = transform.clean_census_data(census_raw)
    outage_clean = transform.clean_outage_data(outage_raw)

    logger.info("PHASE 3: Merging datasets and calculating statistics...")
    
    master_df = load.create_master_dataset(
        ufo_clean, outage_clean, plant_clean, census_clean
    )
    
    if not master_df.empty:
        load.save_dataset(master_df, args.output)
        load.print_correlation_report(master_df)
    else:
        logger.error("Pipeline failed to produce a master dataset.")

    logger.info("--- PIPELINE FINISHED ---")

if __name__ == "__main__":
    main()