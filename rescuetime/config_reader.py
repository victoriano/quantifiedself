#!/usr/bin/env python3
"""
Helper module to read and parse the configuration file for RescueTime scripts.
"""

import os
import sys
import yaml
from datetime import datetime

def load_config(config_file="config.yml"):
    """
    Load and validate the configuration file.
    
    Args:
        config_file (str): Path to the configuration file
        
    Returns:
        dict: Configuration dictionary with processed values
    """
    # Check if config file exists
    if not os.path.exists(config_file):
        print(f"Error: Configuration file '{config_file}' not found.")
        print(f"Please create one based on the template or specify a different file.")
        sys.exit(1)
    
    # Load the YAML file
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading configuration file: {e}")
        sys.exit(1)
    
    # Validate required sections
    required_sections = ['dates', 'domains', 'groups']
    for section in required_sections:
        if section not in config:
            print(f"Error: Required section '{section}' not found in configuration file.")
            sys.exit(1)
    
    # Validate date format
    try:
        start_date = config['dates']['start_date']
        end_date = config['dates']['end_date']
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except KeyError:
        print("Error: Missing start_date or end_date in configuration file.")
        sys.exit(1)
    except ValueError:
        print("Error: Dates must be in YYYY-MM-DD format.")
        sys.exit(1)
    
    # Validate domains
    if not config['domains']:
        print("Error: At least one domain must be specified in the configuration file.")
        sys.exit(1)
    
    for domain in config['domains']:
        if 'name' not in domain:
            print("Error: Each domain must have a 'name' field.")
            sys.exit(1)
        if 'group' not in domain:
            print(f"Error: Domain '{domain['name']}' must have a 'group' field.")
            sys.exit(1)
    
    # Validate and process groups
    if not config['groups']:
        print("Error: At least one group must be specified in the configuration file.")
        sys.exit(1)
    
    # Build maps of group and subgroup configs for quick lookup
    group_map = {}
    for group in config['groups']:
        if 'name' not in group:
            print("Error: Each group must have a 'name' field.")
            sys.exit(1)
        if 'output_dir' not in group:
            print(f"Error: Group '{group['name']}' must have an 'output_dir' field.")
            sys.exit(1)
        if 'output_file' not in group:
            print(f"Error: Group '{group['name']}' must have an 'output_file' field.")
            sys.exit(1)
        
        group_map[group['name']] = group
    
    # Validate parent-child relationships between groups
    for group in config['groups']:
        if 'parent' in group:
            parent_name = group['parent']
            if parent_name not in group_map:
                print(f"Error: Group '{group['name']}' references unknown parent group '{parent_name}'.")
                sys.exit(1)
            # Mark this as a subgroup
            group['is_subgroup'] = True
            # Add this subgroup to the parent's subgroups list
            if 'subgroups' not in group_map[parent_name]:
                group_map[parent_name]['subgroups'] = []
            group_map[parent_name]['subgroups'].append(group['name'])
        else:
            # Main group (not a subgroup)
            group['is_subgroup'] = False
            group['subgroups'] = []  # Initialize empty subgroups list
    
    # Validate that all domain groups and subgroups exist
    for domain in config['domains']:
        group_name = domain['group']
        if group_name not in group_map:
            print(f"Error: Domain '{domain['name']}' references unknown group '{group_name}'.")
            sys.exit(1)
        
        if 'subgroup' in domain:
            subgroup_name = domain['subgroup']
            if subgroup_name not in group_map:
                print(f"Error: Domain '{domain['name']}' references unknown subgroup '{subgroup_name}'.")
                sys.exit(1)
            if 'parent' not in group_map[subgroup_name] or group_map[subgroup_name]['parent'] != group_name:
                print(f"Error: Subgroup '{subgroup_name}' is not a child of group '{group_name}'.")
                sys.exit(1)
    
    # Add domain tags for easy filtering
    for domain in config['domains']:
        group_name = domain['group']
        domain['group_config'] = group_map[group_name]
        
        if 'subgroup' in domain:
            subgroup_name = domain['subgroup']
            domain['subgroup_config'] = group_map[subgroup_name]
            # For output file purposes, use the subgroup's settings
            domain['output_dir'] = group_map[subgroup_name]['output_dir']
            domain['output_file'] = group_map[subgroup_name]['output_file']
        else:
            # No subgroup, use the main group's settings
            domain['output_dir'] = group_map[group_name]['output_dir']
            domain['output_file'] = group_map[group_name]['output_file']
    
    # Set default settings if not specified
    if 'settings' not in config:
        config['settings'] = {}
    
    default_settings = {
        'chunk_months': 3,
        'detailed_data': True,
        'combine_all': True,
        'combine_by_group': True
    }
    
    for key, default_value in default_settings.items():
        if key not in config['settings']:
            config['settings'][key] = default_value
    
    # Process 'all' group if combine_all is enabled
    if config['settings']['combine_all']:
        # Make sure 'all' group exists
        if 'all' not in group_map:
            print("Warning: combine_all is enabled but 'all' group is not defined. Adding default 'all' group.")
            config['groups'].append({
                'name': 'all',
                'output_dir': 'rescuetime_data',
                'output_file': 'all_domains_history.parquet',
                'is_subgroup': False,
                'subgroups': []
            })
            group_map['all'] = config['groups'][-1]
    
    # Add maps to config for easy access
    config['group_map'] = group_map
    
    # Create categorized domain lists
    domain_by_group = {}
    domain_by_subgroup = {}
    
    for domain in config['domains']:
        group = domain['group']
        if group not in domain_by_group:
            domain_by_group[group] = []
        domain_by_group[group].append(domain)
        
        if 'subgroup' in domain:
            subgroup = domain['subgroup']
            if subgroup not in domain_by_subgroup:
                domain_by_subgroup[subgroup] = []
            domain_by_subgroup[subgroup].append(domain)
    
    config['domain_by_group'] = domain_by_group
    config['domain_by_subgroup'] = domain_by_subgroup
    
    return config

if __name__ == "__main__":
    # If run directly, print the loaded configuration for debugging
    import pprint
    
    config = load_config()
    print("Loaded configuration:")
    pprint.pprint(config) 