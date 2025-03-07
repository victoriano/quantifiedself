# Quantified Self

A collection of tools and scripts for tracking and analyzing personal data.

## Projects

### RescueTime Data Pipeline

Tools for fetching, organizing, and analyzing time tracking data from RescueTime with hierarchical categorization.

- **Features:** Group domains by categories (work, social) and subcategories
- **Data Format:** Parquet files for efficient storage and analysis
- **View Details:** [RescueTime Pipeline Documentation](rescuetime/README.md)

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/victoriano/quantifiedself.git
   cd quantifiedself
   ```

2. Create a virtual environment and install dependencies using UV:
   ```bash
   # Create a virtual environment
   uv venv
   
   # Activate the virtual environment
   source .venv/bin/activate
   
   # Install dependencies
   uv pip install -r requirements.txt
   ```

3. Set up API credentials in `.env` file:
   ```
   RESCUETIME_API_KEY=your_api_key_here
   ```

## Usage

Each project has its own dedicated README with detailed instructions:

- [RescueTime Data Pipeline](rescuetime/README.md)

## Requirements

- Python 3.7+
- UV for Python package management
- Required packages are listed in `requirements.txt` 