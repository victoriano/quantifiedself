# RescueTime Data Pipeline

A configurable data pipeline for fetching, organizing, and analyzing time tracking data from RescueTime.

## Features

- **Hierarchical categorization**: Group domains into categories (work, social, etc.) and subcategories
- **Automated data fetching**: Split long date ranges into manageable chunks
- **Flexible data combination**: Combine data at group, subgroup, or all levels
- **Configurable**: Single YAML configuration file for all settings

## Quick Start

1. Edit the `config.yml` file to set up your domains, groups and date ranges
2. Run the full pipeline with a single command:

```bash
uv run main.py
```

## Configuration

The pipeline is configured using `config.yml`. Here's a sample structure:

```yaml
# Date range for data collection
dates:
  start_date: "2023-01-01"
  end_date: "2025-03-06"
  
# Domains to track with hierarchical categorization
domains:
  - name: "graphext.com"
    group: "work"
    subgroup: "graphext"
  
  - name: "x.com"
    group: "social"
    subgroup: "twitter"
  
  - name: "twitter.com"
    group: "social"
    subgroup: "twitter"

# Group/subgroup output settings
groups:
  # Main groups
  - name: "work"
    output_dir: "rescuetime_data"
    output_file: "work_history.parquet"
    
  - name: "social"
    output_dir: "rescuetime_data"
    output_file: "social_history.parquet"

  # Subgroups
  - name: "graphext"
    parent: "work"
    output_dir: "rescuetime_data"
    output_file: "graphext_history.parquet"
```

## Command-Line Options

The main script supports various command-line options:

```
usage: main.py [-h] [--config CONFIG] [--domain DOMAIN] [--group GROUP]
              [--subgroup SUBGROUP] [--skip-fetch] [--skip-combine]
              [--skip-subgroups] [--skip-groups] [--skip-all]

Run the full RescueTime data pipeline using configuration file

options:
  -h, --help           show this help message and exit
  --config CONFIG      Path to configuration file (default: config.yml)
  --domain DOMAIN      Process only this specific domain (optional)
  --group GROUP        Process only domains in this group (optional)
  --subgroup SUBGROUP  Process only domains in this subgroup (optional)
  --skip-fetch         Skip the data fetching phase
  --skip-combine       Skip the data combination phase
  --skip-subgroups     Skip combining data by subgroups
  --skip-groups        Skip combining data by main groups
  --skip-all           Skip combining all domains together
```

## Examples

### Run the full pipeline with default settings

```bash
uv run main.py
```

### Only fetch data for work domains, skip combining

```bash
uv run main.py --group work --skip-combine
```

### Only combine existing data (skip fetching)

```bash
uv run main.py --skip-fetch
```

### Only fetch and process Twitter-related domains

```bash
uv run main.py --subgroup twitter
```

## Output Structure

The pipeline will create several Parquet files:

1. One file per subgroup (e.g., graphext_history.parquet)
2. One file per main group (e.g., work_history.parquet)
3. One combined file with all domains (all_domains_history.parquet)

All files are created in the directories specified in the configuration. 