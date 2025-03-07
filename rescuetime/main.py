#!/usr/bin/env python3
"""
Main script to run the full RescueTime data pipeline:
1. Fetch data for all domains in chunks
2. Combine data according to hierarchical group structure

This script uses config.yml for all settings and can be run with a single command.
"""

import os
import sys
import argparse
import time
from datetime import datetime
from config_reader import load_config

# Import functions from other modules
from fetch_long_period import fetch_domain_data, generate_date_ranges
from combine_parquet import combine_parquet_files, print_data_summary, collect_domain_files

def timestamp():
    """Return current timestamp for logging"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def fetch_all_data(config, args):
    """
    Fetch data for all domains in the configuration
    
    Args:
        config (dict): Loaded configuration
        args (argparse.Namespace): Command line arguments
    
    Returns:
        dict: Mapping of domain names to lists of output files
    """
    print(f"\n[{timestamp()}] STARTING DATA FETCHING PHASE")
    
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
    for i, domain in enumerate(domains_to_process):
        print(f"\n[{timestamp()}] Processing domain {i+1}/{len(domains_to_process)}: {domain['name']}")
        output_files = fetch_domain_data(domain, start_date, end_date, chunk_months)
        all_domains_files[domain['name']] = output_files
    
    print(f"\n[{timestamp()}] DATA FETCHING PHASE COMPLETE")
    print(f"Fetched data for {len(domains_to_process)} domains.")
    
    return all_domains_files

def combine_data(config, args):
    """
    Combine data files according to the hierarchical structure
    
    Args:
        config (dict): Loaded configuration
        args (argparse.Namespace): Command line arguments
    """
    print(f"\n[{timestamp()}] STARTING DATA COMBINATION PHASE")
    
    # Extract settings
    combine_all = config['settings']['combine_all']
    combine_by_group = config['settings']['combine_by_group']
    
    files_combined = 0
    output_files = []
    
    # Step 1: Process each subgroup
    if not args.skip_subgroups:
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
            
            print(f"\n[{timestamp()}] Processing subgroup '{group_name}'")
            
            # Collect all files for this subgroup
            all_files = collect_domain_files(domains)
            
            if not all_files:
                print(f"No Parquet files found for subgroup '{group_name}'. Skipping.")
                continue
            
            # Determine output file path
            output_dir = group['output_dir']
            output_file = os.path.join(output_dir, group['output_file'])
            
            print(f"Found {len(all_files)} Parquet files to combine for subgroup '{group_name}'.")
            
            # Combine the files
            combined_df, _ = combine_parquet_files(all_files, output_file)
            files_combined += len(all_files)
            output_files.append(output_file)
            
            # Print summary
            print_data_summary(combined_df, domain_names, "subgroup", group_name)
    
    # Step 2: Process each main group if combine_by_group is enabled
    if combine_by_group and not args.skip_groups:
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
            
            print(f"\n[{timestamp()}] Processing main group '{group_name}'")
            
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
            files_combined += len(all_files)
            output_files.append(output_file)
            
            # Print summary
            print_data_summary(combined_df, domain_names, "group", group_name)
    
    # Step 3: Process ALL domains if combine_all is enabled
    if combine_all and not args.skip_all:
        all_domains = config['domains']
        all_domain_names = [d['name'] for d in all_domains]
        
        # Get the 'all' group configuration
        all_group = config['group_map'].get('all')
        if not all_group:
            print("Warning: 'all' group not found in configuration. Skipping all-domains combination.")
            return
        
        print(f"\n[{timestamp()}] Processing all domains together")
        
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
        files_combined += len(all_files)
        output_files.append(output_file)
        
        # Print summary
        print_data_summary(combined_df, all_domain_names, "all domains", None)
    
    print(f"\n[{timestamp()}] DATA COMBINATION PHASE COMPLETE")
    print(f"Combined {files_combined} raw data files into {len(output_files)} output files:")
    for output_file in output_files:
        print(f"- {output_file}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Run the full RescueTime data pipeline using configuration file"
    )
    parser.add_argument(
        "--config", 
        default="config.yml", 
        help="Path to configuration file (default: config.yml)"
    )
    parser.add_argument(
        "--domain", 
        help="Process only this specific domain (optional)"
    )
    parser.add_argument(
        "--group", 
        help="Process only domains in this group (optional)"
    )
    parser.add_argument(
        "--subgroup", 
        help="Process only domains in this subgroup (optional)"
    )
    parser.add_argument(
        "--skip-fetch", 
        action="store_true", 
        help="Skip the data fetching phase"
    )
    parser.add_argument(
        "--skip-combine", 
        action="store_true", 
        help="Skip the data combination phase"
    )
    parser.add_argument(
        "--skip-subgroups", 
        action="store_true", 
        help="Skip combining data by subgroups"
    )
    parser.add_argument(
        "--skip-groups", 
        action="store_true", 
        help="Skip combining data by main groups"
    )
    parser.add_argument(
        "--skip-all", 
        action="store_true", 
        help="Skip combining all domains together"
    )
    
    args = parser.parse_args()
    
    # Record start time
    start_time = time.time()
    
    # Load the configuration
    print(f"[{timestamp()}] Loading configuration from {args.config}")
    config = load_config(args.config)
    
    # Print pipeline information
    domain_count = len(config['domains'])
    group_count = sum(1 for g in config['groups'] if not g.get('is_subgroup', False) and g['name'] != 'all')
    subgroup_count = sum(1 for g in config['groups'] if g.get('is_subgroup', False))
    
    print(f"[{timestamp()}] Pipeline configured for:")
    print(f"- {domain_count} domains")
    print(f"- {group_count} main groups")
    print(f"- {subgroup_count} subgroups")
    print(f"- Date range: {config['dates']['start_date']} to {config['dates']['end_date']}")
    
    # Run the pipeline
    if not args.skip_fetch:
        fetch_all_data(config, args)
    else:
        print(f"\n[{timestamp()}] Skipping data fetching phase")
    
    if not args.skip_combine:
        combine_data(config, args)
    else:
        print(f"\n[{timestamp()}] Skipping data combination phase")
    
    # Report completion
    elapsed_time = time.time() - start_time
    print(f"\n[{timestamp()}] PIPELINE COMPLETE")
    print(f"Total execution time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")

if __name__ == "__main__":
    main() 