#!/usr/bin/env python3
"""
Script to combine multiple Parquet files into a single file.
Uses configuration from config.yml to determine which files to combine.
Supports hierarchical group structure for organizing domains.
"""

import os
import glob
import argparse
import polars as pl
from config_reader import load_config
from datetime import datetime

def combine_parquet_files(files, output_file):
    """
    Combine multiple Parquet files into a single file.
    
    Args:
        files (list): List of Parquet files to combine
        output_file (str): Output file path
    
    Returns:
        tuple: (combined_df, total_rows) - the combined dataframe and row count
    """
    if not files:
        print("No files to combine.")
        return None, 0
    
    # Read and combine all files
    dfs = []
    total_rows = 0
    required_columns = ["group", "subgroup"]
    
    # First pass: read all dataframes and collect all column names to create a unified schema
    all_columns = set()
    
    for file in files:
        try:
            df = pl.read_parquet(file)
            all_columns.update(df.columns)
        except Exception as e:
            print(f"Error reading {file} during schema discovery: {e}")
    
    print(f"Unified schema will include these columns: {sorted(list(all_columns))}")
    
    # Second pass: read again and ensure all dataframes have the same columns
    for file in files:
        try:
            df = pl.read_parquet(file)
            
            # Add missing columns with default values
            for col in all_columns:
                if col not in df.columns:
                    # Choose appropriate default values based on column name
                    if col in ["group", "subgroup"]:
                        df = df.with_columns(pl.lit("unknown").alias(col))
                    elif "seconds" in col.lower() or col == "Productivity":
                        df = df.with_columns(pl.lit(0).alias(col))
                    elif col == "Number of People":
                        df = df.with_columns(pl.lit(1).alias(col))
                    else:
                        df = df.with_columns(pl.lit("").alias(col))
            
            rows = df.height
            total_rows += rows
            print(f"Read {file}: {rows} rows")
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")
    
    if not dfs:
        print("No valid data files to combine.")
        return None, 0
    
    # Concatenate all dataframes
    print("\nCombining data...")
    combined_df = pl.concat(dfs)
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    
    # Print the schema of the combined dataframe
    print("\nSchema of combined data:")
    for col in combined_df.columns:
        dtype = combined_df.schema[col]
        print(f"- {col}: {dtype}")
    
    combined_df.write_parquet(output_file)
    print(f"Combined data saved to {output_file}")
    print(f"Total rows: {total_rows} (from individual files)")
    print(f"Combined rows: {combined_df.height}")
    
    return combined_df, total_rows

def print_data_summary(df, domain_names, category_type=None, category_name=None):
    """
    Print a summary of the combined data.
    
    Args:
        df (polars.DataFrame): The combined dataframe
        domain_names (list): List of domain names included in this data
        category_type (str): Type of category (group, subgroup, all)
        category_name (str): Name of the category
    """
    if df is None or df.is_empty():
        print("No data to summarize.")
        return
    
    category_info = ""
    if category_type and category_name:
        category_info = f" for {category_type} '{category_name}'"
    
    print(f"\nData Summary{category_info}:")
    min_date = df["Date"].min()
    max_date = df["Date"].max()
    print(f"Date range: {min_date} to {max_date}")
    
    # Calculate total hours spent
    total_seconds = df["Time Spent (seconds)"].sum()
    total_hours = total_seconds / 3600
    print(f"Total time spent: {total_hours:.2f} hours across {', '.join(domain_names)}")
    
    # Check if group and subgroup columns exist
    if "group" in df.columns and "subgroup" in df.columns:
        # Show time breakdown by group/subgroup
        print("\nTime breakdown by category:")
        group_summary = df.group_by(["group", "subgroup"]).agg(
            pl.sum("Time Spent (seconds)").alias("total_seconds")
        ).with_columns(
            (pl.col("total_seconds") / 3600).alias("hours")
        ).sort(pl.col("total_seconds"), descending=True)
        
        print(group_summary)
    
    # Top days with most activity (aggregating by date)
    try:
        date_only = df.with_columns(
            pl.col("Date").str.slice(0, 10).alias("DateOnly")
        )
        
        top_days = date_only.group_by("DateOnly").agg(
            pl.sum("Time Spent (seconds)").alias("total_seconds")
        ).with_columns(
            (pl.col("total_seconds") / 3600).alias("hours")
        ).sort(pl.col("total_seconds"), descending=True).head(5)
        
        print("\nTop 5 days with most activity:")
        print(top_days)
    except Exception as e:
        print(f"Could not compute top days: {e}")

def collect_domain_files(domains, domain_base_names=None):
    """
    Collect all data files for a list of domains.
    
    Args:
        domains (list): List of domain configurations
        domain_base_names (dict, optional): Optional mapping of domain name to base name for file matching
        
    Returns:
        list: List of Parquet file paths
    """
    all_files = []
    
    if domain_base_names is None:
        domain_base_names = {}
    
    # First, delete any previously generated combined files to avoid recursion
    for domain in domains:
        if 'output_file' in domain and os.path.exists(os.path.join(domain.get('output_dir', ''), domain['output_file'])):
            try:
                os.remove(os.path.join(domain.get('output_dir', ''), domain['output_file']))
                print(f"Removed previous combined file: {os.path.join(domain.get('output_dir', ''), domain['output_file'])}")
            except Exception as e:
                print(f"Warning: Could not remove previous combined file: {e}")
    
    # Now collect new files
    for domain in domains:
        data_dir = domain['output_dir']
        
        # Check if directory exists
        if not os.path.exists(data_dir):
            print(f"Warning: Directory '{data_dir}' for domain '{domain['name']}' does not exist. Skipping.")
            continue
        
        # Use specific base name if provided, otherwise extract from domain name
        if domain['name'] in domain_base_names:
            domain_base = domain_base_names[domain['name']]
        else:
            domain_base = domain['name'].split('.')[0]
        
        # Find all Parquet files for this domain
        parquet_files = glob.glob(f"{data_dir}/{domain_base}_*.parquet")
        
        # Sort by modification time to use the most recent files
        parquet_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        
        # Print information about found files
        if parquet_files:
            newest_file = parquet_files[0]
            newest_time = datetime.fromtimestamp(os.path.getmtime(newest_file))
            print(f"Found {len(parquet_files)} files for domain '{domain['name']}'. Most recent: {newest_file} ({newest_time})")
        
        all_files.extend(parquet_files)
    
    return all_files

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Combine multiple Parquet files based on hierarchical configuration")
    parser.add_argument("--config", default="config.yml", help="Path to configuration file (default: config.yml)")
    args = parser.parse_args()
    
    # Load the configuration
    config = load_config(args.config)
    
    # Extract settings
    combine_all = config['settings']['combine_all']
    combine_by_group = config['settings']['combine_by_group']
    
    # Mapping to track which domain files should be combined into which output files
    # This helps avoid duplicate processing
    processed_domains = set()
    
    # Step 1: Process each subgroup
    for group_name, group in config['group_map'].items():
        # Skip the "all" group and non-subgroups
        if group_name == 'all' or not group.get('is_subgroup', False):
            continue
        
        # Get the domains in this subgroup
        if group_name not in config['domain_by_subgroup']:
            print(f"No domains found for subgroup '{group_name}'. Skipping.")
            continue
            
        domains = config['domain_by_subgroup'][group_name]
        domain_names = [d['name'] for d in domains]
        
        print(f"\nProcessing subgroup '{group_name}'")
        
        # Collect all files for this subgroup
        all_files = collect_domain_files(domains)
        
        if not all_files:
            print(f"No Parquet files found for subgroup '{group_name}'. Skipping.")
            continue
        
        # Mark these domains as processed
        processed_domains.update(domain_names)
        
        # Determine output file path
        output_dir = group['output_dir']
        output_file = os.path.join(output_dir, group['output_file'])
        
        print(f"Found {len(all_files)} Parquet files to combine for subgroup '{group_name}'.")
        
        # Combine the files
        combined_df, _ = combine_parquet_files(all_files, output_file)
        
        # Print summary
        print_data_summary(combined_df, domain_names, "subgroup", group_name)
    
    # Step 2: Process each main group if combine_by_group is enabled
    if combine_by_group:
        for group_name, group in config['group_map'].items():
            # Skip subgroups and the "all" group
            if group.get('is_subgroup', False) or group_name == 'all':
                continue
            
            # Get the domains in this group
            if group_name not in config['domain_by_group']:
                print(f"No domains found for group '{group_name}'. Skipping.")
                continue
                
            domains = config['domain_by_group'][group_name]
            domain_names = [d['name'] for d in domains]
            
            print(f"\nProcessing main group '{group_name}'")
            
            # Collect all files for this group
            all_files = collect_domain_files(domains)
            
            if not all_files:
                print(f"No Parquet files found for group '{group_name}'. Skipping.")
                continue
            
            # Determine output file path
            output_dir = group['output_dir']
            output_file = os.path.join(output_dir, group['output_file'])
            
            print(f"Found {len(all_files)} Parquet files to combine for group '{group_name}'.")
            
            # Combine the files
            combined_df, _ = combine_parquet_files(all_files, output_file)
            
            # Print summary
            print_data_summary(combined_df, domain_names, "group", group_name)
    
    # Step 3: Process ALL domains if combine_all is enabled
    if combine_all:
        all_domains = config['domains']
        all_domain_names = [d['name'] for d in all_domains]
        
        # Get the 'all' group configuration
        all_group = config['group_map'].get('all')
        if not all_group:
            print("Warning: 'all' group not found in configuration. Skipping all-domains combination.")
            return
        
        print("\nProcessing all domains together")
        
        # Collect all files for all domains
        all_files = collect_domain_files(all_domains)
        
        if not all_files:
            print("No Parquet files found for any domain. Skipping all-domains combination.")
            return
        
        # Determine output file path
        output_dir = all_group['output_dir']
        output_file = os.path.join(output_dir, all_group['output_file'])
        
        print(f"Found {len(all_files)} Parquet files to combine for all domains.")
        
        # Combine the files
        combined_df, _ = combine_parquet_files(all_files, output_file)
        
        # Print summary
        print_data_summary(combined_df, all_domain_names, "all domains", None)

if __name__ == "__main__":
    main() 