#!/usr/bin/env python3
"""
Script to fetch RescueTime data for a long period by breaking it into 3-month chunks.
Uses configuration from config.yml for date ranges and domain groups/subgroups.
"""

import os
import sys
import subprocess
import argparse
from datetime import datetime, timedelta
import calendar
from config_reader import load_config

def add_months(sourcedate, months):
    """
    Add a number of months to a date, handling year rollovers correctly.
    """
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year, month)[1])
    return datetime(year, month, day)

def generate_date_ranges(start_date, end_date, chunk_months=3):
    """
    Generate date ranges in 3-month chunks between start_date and end_date.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        chunk_months (int): Number of months per chunk
        
    Returns:
        list: List of (chunk_start, chunk_end) tuples
    """
    # Convert string dates to datetime objects
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Initialize result list and current date
    date_ranges = []
    current = start
    
    # Generate chunks
    while current < end:
        # Calculate chunk end date (3 months from current)
        chunk_end = add_months(current, chunk_months)
        
        # Ensure we don't go beyond the overall end date
        if chunk_end > end:
            chunk_end = end
        
        # Add the date range to our list
        date_ranges.append((
            current.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d")
        ))
        
        # Move to the next chunk
        current = add_months(chunk_end, 1)
        
        # Ensure we don't get stuck in an infinite loop
        if current <= chunk_end:
            current = chunk_end + timedelta(days=1)
    
    return date_ranges

def fetch_domain_data(domain, start_date, end_date, chunk_months):
    """
    Fetch data for a specific domain in chunks.
    
    Args:
        domain (dict): Domain configuration from config file
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        chunk_months (int): Number of months per chunk
        
    Returns:
        list: List of output files created
    """
    domain_name = domain['name']
    domain_base = domain_name.split('.')[0]
    output_dir = domain['output_dir']
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate date ranges in chunks
    date_ranges = generate_date_ranges(start_date, end_date, chunk_months)
    
    # Build category info for logging
    category_info = f"domain '{domain_name}'"
    if 'group' in domain and 'subgroup' in domain:
        category_info = f"domain '{domain_name}' (group: {domain['group']}, subgroup: {domain['subgroup']})"
    elif 'group' in domain:
        category_info = f"domain '{domain_name}' (group: {domain['group']})"
    
    print(f"\nFetching data for {category_info} from {start_date} to {end_date} in {len(date_ranges)} chunks:")
    for i, (chunk_start, chunk_end) in enumerate(date_ranges):
        print(f"  Chunk {i+1}: {chunk_start} to {chunk_end}")
    
    # Process each chunk
    output_files = []
    for i, (chunk_start, chunk_end) in enumerate(date_ranges):
        print(f"\nProcessing chunk {i+1}/{len(date_ranges)}: {chunk_start} to {chunk_end}")
        
        # Create output filename for this chunk
        output_file = f"{output_dir}/{domain_base}_{chunk_start}_to_{chunk_end}.parquet"
        output_files.append(output_file)
        
        # Build the command with additional parameters for category information
        cmd = [
            "uv", "run", "rescuetime_graphext.py",
            "--raw",
            "--start", chunk_start,
            "--end", chunk_end,
            "--domain", domain_name,
            "--raw-output", output_file
        ]
        
        # Add group/subgroup info to the command if available
        if 'group' in domain:
            cmd.extend(["--group", domain['group']])
        if 'subgroup' in domain:
            cmd.extend(["--subgroup", domain['subgroup']])
        
        # Execute the command
        print(f"Running: {' '.join(cmd)}")
        subprocess.run(cmd)
    
    print("\nAll chunks processed. Data saved in the following files:")
    for output_file in output_files:
        if os.path.exists(output_file):
            print(f"  - {output_file}")
    
    return output_files

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Fetch RescueTime data over a long period using configuration file")
    parser.add_argument("--config", default="config.yml", help="Path to configuration file (default: config.yml)")
    parser.add_argument("--domain", help="Process only this specific domain (optional)")
    parser.add_argument("--group", help="Process only domains in this group (optional)")
    parser.add_argument("--subgroup", help="Process only domains in this subgroup (optional)")
    args = parser.parse_args()
    
    # Load the configuration
    config = load_config(args.config)
    
    # Get dates and settings
    start_date = config['dates']['start_date']
    end_date = config['dates']['end_date']
    chunk_months = config['settings']['chunk_months']
    
    # Filter domains based on command line arguments
    domains_to_process = []
    
    if args.domain:
        # Find the specific domain
        for domain in config['domains']:
            if domain['name'] == args.domain:
                domains_to_process.append(domain)
                break
        if not domains_to_process:
            print(f"Error: Domain '{args.domain}' not found in configuration.")
            sys.exit(1)
    elif args.group and args.subgroup:
        # Find domains in this group and subgroup
        for domain in config['domains']:
            if domain.get('group') == args.group and domain.get('subgroup') == args.subgroup:
                domains_to_process.append(domain)
        if not domains_to_process:
            print(f"Error: No domains found in group '{args.group}', subgroup '{args.subgroup}'.")
            sys.exit(1)
    elif args.group:
        # Find domains in this group
        for domain in config['domains']:
            if domain.get('group') == args.group:
                domains_to_process.append(domain)
        if not domains_to_process:
            print(f"Error: No domains found in group '{args.group}'.")
            sys.exit(1)
    elif args.subgroup:
        # Find domains in this subgroup
        for domain in config['domains']:
            if domain.get('subgroup') == args.subgroup:
                domains_to_process.append(domain)
        if not domains_to_process:
            print(f"Error: No domains found in subgroup '{args.subgroup}'.")
            sys.exit(1)
    else:
        # Process all domains
        domains_to_process = config['domains']
    
    # Process each domain
    all_domains_files = {}
    for domain in domains_to_process:
        output_files = fetch_domain_data(domain, start_date, end_date, chunk_months)
        all_domains_files[domain['name']] = output_files
    
    print("\nData fetching complete for all selected domains.")
    print("To combine files based on groups and subgroups, run:")
    print("  uv run combine_parquet.py")

if __name__ == "__main__":
    main() 