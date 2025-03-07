#!/usr/bin/env python3
"""
RescueTime Graphext.com Time Tracker

This script fetches data from the RescueTime API to analyze time spent on graphext.com.
Supports Parquet output for efficient data storage and analytics integration.
Uses Polars for high-performance data processing.
"""

import os
import sys
import json
import datetime
from datetime import timedelta
import requests
import polars as pl
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get API key from environment
API_KEY = os.getenv("RESCUETIME_API_KEY")
if not API_KEY:
    print("Error: RESCUETIME_API_KEY not found in .env file")
    print("Please create a .env file with your RescueTime API key")
    sys.exit(1)

class RescueTimeAPI:
    """Class to interact with the RescueTime API"""
    
    # Base URL for the Analytics API Data endpoint
    BASE_URL = "https://www.rescuetime.com/anapi/data"
    
    def __init__(self, api_key):
        self.api_key = api_key
    
    def get_raw_detailed_data(self, domain="graphext.com", start_date=None, end_date=None, 
                              include_timestamps=True, output_format="parquet"):
        """
        Retrieve the most granular, disaggregated data available from RescueTime API
        for a specific domain. Optimized for data pipeline integration (e.g., Dagster).
        
        Args:
            domain (str): Domain to filter data for (default: graphext.com)
            start_date (str): Start date in YYYY-MM-DD format (default: 1 day ago)
            end_date (str): End date in YYYY-MM-DD format (default: today)
            include_timestamps (bool): Include timestamp when the data was fetched
            output_format (str): Format of output - 'polars', 'parquet', 'dict', or 'json'
            
        Returns:
            Data in the requested format (Polars DataFrame, ParquetBytes, dict, or JSON string)
        """
        # Set default dates if not provided - just 1 day by default for more granular data
        if not start_date:
            current_date = datetime.datetime.now()
            start_date = (current_date - timedelta(days=1)).strftime("%Y-%m-%d")
        if not end_date:
            current_date = datetime.datetime.now()
            end_date = current_date.strftime("%Y-%m-%d")
        
        # Add timestamp for data lineage in pipelines
        fetch_timestamp = datetime.datetime.now().isoformat()
        
        # Prepare parameters for maximum disaggregation
        params = {
            "key": self.api_key,
            "perspective": "interval",  # Most detailed view
            "resolution_time": "hour",  # Hourly resolution (most granular available)
            "restrict_begin": start_date,
            "restrict_end": end_date,
            "format": "json"
        }
        
        print(f"Fetching detailed data from {start_date} to {end_date}...")
        
        try:
            response = requests.get(self.BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Create a Polars DataFrame from the response
            df = pl.DataFrame(data["rows"], schema=data["row_headers"])
            
            # Try to filter for the domain in any relevant column
            domain_columns = ["Document", "Activity", "Category", "Description"]
            
            # Find columns that actually exist in the dataframe
            available_columns = [col for col in domain_columns if col in df.columns]
            
            # Filtering with Polars
            if available_columns:
                # Create a filter expression for domain matching in any available column
                filter_expr = None
                for col in available_columns:
                    # Convert both strings to lowercase for case-insensitive comparison
                    expr = pl.col(col).str.to_lowercase().str.contains(domain.lower())
                    if filter_expr is None:
                        filter_expr = expr
                    else:
                        filter_expr = filter_expr | expr
                
                # Apply the filter if we have a valid expression
                if filter_expr is not None:
                    filtered_df = df.filter(filter_expr)
                else:
                    filtered_df = pl.DataFrame(schema=df.schema)
            else:
                # Empty dataframe with same schema
                filtered_df = pl.DataFrame(schema=df.schema)
            
            if filtered_df.is_empty():
                print(f"No data found for domain '{domain}'")
                print("Available columns:", df.columns)
                print("Sample data (first few rows):")
                print(df.head())
                # Return empty result in requested format
                if output_format in ["polars", "parquet"]:
                    return pl.DataFrame()
                elif output_format == "dict":
                    return {"data": [], "metadata": {"query_domain": domain, "timestamp": fetch_timestamp}}
                else:  # json
                    return json.dumps({"data": [], "metadata": {"query_domain": domain, "timestamp": fetch_timestamp}})
            
            # Add metadata for data pipeline traceability
            if include_timestamps:
                filtered_df = filtered_df.with_columns([
                    pl.lit(fetch_timestamp).alias("fetch_timestamp"),
                    pl.lit(domain).alias("query_domain")
                ])
            
            # Return in requested format
            if output_format == "polars":
                return filtered_df
            elif output_format == "parquet":
                # Polars handles Parquet well without conversion needed
                return filtered_df
            elif output_format == "dict":
                # Convert to dictionary format
                result = {
                    "data": filtered_df.to_dicts(),
                    "metadata": {
                        "query_domain": domain,
                        "start_date": start_date,
                        "end_date": end_date,
                        "timestamp": fetch_timestamp,
                        "row_count": filtered_df.height
                    }
                }
                return result
            else:  # json
                result = {
                    "data": filtered_df.to_dicts(),
                    "metadata": {
                        "query_domain": domain,
                        "start_date": start_date,
                        "end_date": end_date,
                        "timestamp": fetch_timestamp,
                        "row_count": filtered_df.height
                    }
                }
                return json.dumps(result)
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from RescueTime API: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response content: {e.response.text}")
            
            # Return empty result in requested format
            if output_format in ["polars", "parquet"]:
                return pl.DataFrame()
            elif output_format == "dict":
                return {"data": [], "error": str(e), "metadata": {"query_domain": domain, "timestamp": fetch_timestamp}}
            else:  # json
                return json.dumps({"data": [], "error": str(e), "metadata": {"query_domain": domain, "timestamp": fetch_timestamp}})
    
    def fetch_data(self, start_date=None, end_date=None, domain="graphext.com"):
        """
        Fetch data from RescueTime API for a specific domain
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format (default: 7 days ago)
            end_date (str): End date in YYYY-MM-DD format (default: today)
            domain (str): Domain to search for (default: graphext.com)
            
        Returns:
            polars.DataFrame: Data frame with time data
        """
        # Set default dates if not provided - use a shorter time range (7 days)
        if not start_date:
            current_date = datetime.datetime.now()
            start_date = (current_date - timedelta(days=7)).strftime("%Y-%m-%d")
        if not end_date:
            current_date = datetime.datetime.now()
            end_date = current_date.strftime("%Y-%m-%d")
        
        # Prepare parameters for the API request according to documentation
        params = {
            "key": self.api_key,
            # Use activity detailed view to get domain-level data
            "perspective": "interval",
            "restrict_begin": start_date,
            "restrict_end": end_date,
            "format": "json"
        }
        
        print(f"Fetching data from {start_date} to {end_date}...")
        
        try:
            response = requests.get(self.BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Create a Polars DataFrame from the response
            df = pl.DataFrame(data["rows"], schema=data["row_headers"])
            
            # Filter for graphext.com in the appropriate column
            # Check column names to find the one containing domain info
            domain_columns = ["Document", "Activity", "Category", "Description"]
            
            # Find columns that actually exist in the dataframe
            available_columns = [col for col in domain_columns if col in df.columns]
            
            # Create a combined filter expression
            if available_columns:
                filter_expr = None
                for col in available_columns:
                    # Convert both strings to lowercase for case-insensitive comparison
                    expr = pl.col(col).str.to_lowercase().str.contains(domain.lower())
                    if filter_expr is None:
                        filter_expr = expr
                    else:
                        filter_expr = filter_expr | expr
                    
                if filter_expr is not None:
                    filtered_df = df.filter(filter_expr)
                    
                    # Remove duplicates if any
                    if not filtered_df.is_empty():
                        filtered_df = filtered_df.unique()
                else:
                    filtered_df = pl.DataFrame(schema=df.schema)
            else:
                filtered_df = pl.DataFrame(schema=df.schema)
            
            if filtered_df.is_empty():
                print(f"No data found for {domain}")
                # Return the original dataframe for debugging
                print("Available columns:", df.columns)
                print("Sample data (first few rows):")
                print(df.head())
                return None
            
            return filtered_df
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from RescueTime API: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response content: {e.response.text}")
            return None
    
    def analyze_data(self, df):
        """
        Analyze the fetched data
        
        Args:
            df (polars.DataFrame): Data frame with time data
            
        Returns:
            dict: Analysis results
        """
        if df is None or df.is_empty():
            return None
        
        # Find the time column (could be "Time Spent (seconds)" or similar)
        time_cols = [col for col in df.columns if "time" in col.lower() or "spent" in col.lower()]
        if not time_cols:
            print("Could not find time column in data")
            print("Available columns:", df.columns)
            return None
        
        time_col = time_cols[0]
        
        # Find the date column
        date_cols = [col for col in df.columns if "date" in col.lower()]
        if not date_cols:
            date_cols = ["Date"]  # Default name
        date_col = date_cols[0]
        
        if date_col not in df.columns:
            print(f"Date column '{date_col}' not found in data")
            print("Available columns:", df.columns)
            return None
        
        # Get column names
        print("Working with columns:")
        print(f"- Date column: {date_col}")
        print(f"- Time column: {time_col}")
        
        # Rename columns for clarity
        renamed_df = df.rename({
            date_col: "date",
            time_col: "time_spent"
        })
        
        # Convert Date to datetime
        renamed_df = renamed_df.with_columns(
            pl.col("date").str.to_datetime()
        )
        
        # Group by date and sum time spent
        daily_usage = (renamed_df
            .group_by("date")
            .agg(pl.sum("time_spent").alias("time_spent"))
        )
        
        # Convert seconds to hours
        daily_usage = daily_usage.with_columns(
            (pl.col("time_spent") / 3600).alias("hours")
        )
        
        # Calculate total time spent
        total_time = renamed_df.select(pl.sum("time_spent")).item() / 3600  # in hours
        
        # Calculate average daily time
        avg_daily_time = total_time / daily_usage.height
        
        return {
            "data": daily_usage.to_pandas(),  # Convert back for matplotlib compatibility
            "total_hours": total_time,
            "avg_daily_hours": avg_daily_time
        }
    
    def visualize_data(self, analysis_results, save_path=None):
        """
        Create a visualization of the data
        
        Args:
            analysis_results (dict): Results from analyze_data
            save_path (str): Path to save the visualization
        """
        if not analysis_results:
            print("No data to visualize")
            return
        
        data = analysis_results["data"]
        
        plt.figure(figsize=(12, 6))
        plt.bar(data["date"], data["hours"], color="skyblue")
        plt.xlabel("Date")
        plt.ylabel("Hours Spent")
        plt.title("Time Spent on graphext.com")
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Add annotations for total and average
        total = analysis_results["total_hours"]
        avg = analysis_results["avg_daily_hours"]
        plt.figtext(0.5, 0.01, 
                   f"Total Hours: {total:.2f} | Average Daily Hours: {avg:.2f}",
                   ha="center", fontsize=12, bbox={"facecolor":"orange", "alpha":0.1, "pad":5})
        
        if save_path:
            plt.savefig(save_path)
            print(f"Visualization saved to {save_path}")
        else:
            plt.show()

def main():
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Analyze time spent on graphext.com using RescueTime API")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--domain", default="graphext.com", help="Domain to search for (default: graphext.com)")
    parser.add_argument("--save", help="Save the visualization to a file")
    parser.add_argument("--csv", help="Save the data to a CSV file")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--raw", action="store_true", help="Get raw detailed data (for pipeline integration)")
    parser.add_argument("--raw-format", choices=["polars", "parquet", "dict", "json"], default="parquet", 
                      help="Output format for raw data (default: parquet)")
    parser.add_argument("--raw-output", help="File to save raw output (if using --raw), defaults to graphext_data.parquet")
    
    args = parser.parse_args()
    
    # Initialize the API
    api = RescueTimeAPI(API_KEY)
    
    # Check if we should get raw data (for pipeline integration)
    if args.raw:
        # Set a default output file if not provided
        output_file = args.raw_output
        if not output_file:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"graphext_data_{timestamp}.parquet"
        
        raw_data = api.get_raw_detailed_data(
            domain=args.domain,
            start_date=args.start, 
            end_date=args.end,
            output_format=args.raw_format
        )
        
        # Handle the output based on format
        if args.raw_format in ["polars", "parquet"]:
            if raw_data.is_empty():
                print("No data returned.")
            else:
                print(f"Retrieved {raw_data.height} records.")
                print("\nSample data (first 5 rows):")
                print(raw_data.head())
                
                # Save to file if requested
                if output_file:
                    if output_file.endswith('.csv'):
                        raw_data.write_csv(output_file)
                    elif output_file.endswith('.parquet'):
                        # Polars handles Parquet natively with better performance
                        raw_data.write_parquet(output_file)
                    elif output_file.endswith('.json'):
                        raw_data.write_json(output_file)
                    else:
                        # Default to Parquet
                        if not output_file.endswith('.parquet'):
                            output_file += '.parquet'
                        raw_data.write_parquet(output_file)
                    print(f"Raw data saved to {output_file}")
        else:  # dict or json
            if output_file:
                with open(output_file, 'w') as f:
                    if args.raw_format == "dict":
                        json.dump(raw_data, f)
                    else:  # json string
                        f.write(raw_data)
                print(f"Raw data saved to {output_file}")
            else:
                # Just print a summary
                if args.raw_format == "dict":
                    print(f"Records: {len(raw_data.get('data', []))}")
                    if raw_data.get('metadata'):
                        print(f"Metadata: {raw_data.get('metadata')}")
                else:  # json
                    parsed = json.loads(raw_data)
                    print(f"Records: {len(parsed.get('data', []))}")
                    if parsed.get('metadata'):
                        print(f"Metadata: {parsed.get('metadata')}")
        
        # Exit early - we're done with the raw data
        return
    
    # Standard visualization flow
    df = api.fetch_data(args.start, args.end, args.domain)
    
    if df is not None:
        # Debug: show raw data
        if args.debug:
            print("\nRaw data (first 5 rows):")
            print(df.head())
        
        # Save raw data to CSV if requested
        if args.csv:
            df.write_csv(args.csv)
            print(f"Data saved to {args.csv}")
        
        # Analyze data
        analysis = api.analyze_data(df)
        
        if analysis:
            # Print summary
            print("\nSummary:")
            print("-" * 40)
            print(f"Domain: {args.domain}")
            date_col = [col for col in df.columns if "date" in col.lower() or col == "Date"][0]
            date_min = df.select(pl.min(date_col)).item()
            date_max = df.select(pl.max(date_col)).item()
            print(f"Period: {date_min} to {date_max}")
            print(f"Total time spent: {analysis['total_hours']:.2f} hours")
            print(f"Average daily time: {analysis['avg_daily_hours']:.2f} hours")
            print("-" * 40)
            
            # Visualize data
            api.visualize_data(analysis, args.save)
    else:
        print("No data available for analysis")
        print("\nTrying generic data fetch to debug...")
        # Try to get any data to see the structure
        test_params = {
            "key": API_KEY,
            "perspective": "interval",
            "format": "json",
            "restrict_begin": (datetime.datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
            "restrict_end": datetime.datetime.now().strftime("%Y-%m-%d")
        }
        try:
            response = requests.get(api.BASE_URL, params=test_params)
            response.raise_for_status()
            data = response.json()
            print("API returned data successfully!")
            print("Data structure:", data.keys())
            print("Row headers:", data.get("row_headers", "No headers found"))
            print("Sample row:", data.get("rows", ["No rows found"])[0] if data.get("rows") else "No rows found")
        except Exception as e:
            print(f"Debug fetch also failed: {e}")

# Example of how to use this in a Dagster pipeline
def dagster_pipeline_example():
    """
    Example of how this could be used in a Dagster pipeline.
    This is just illustrative - not meant to be run directly.
    
    In a real Dagster pipeline, you would:
    1. Import this module
    2. Use the get_raw_detailed_data function in an op
    3. Connect that op to an S3 asset or resource
    """
    # This is pseudocode for illustration
    '''
    @op
    def fetch_rescuetime_data():
        """Fetch RescueTime data for graphext.com"""
        api = RescueTimeAPI(os.getenv("RESCUETIME_API_KEY"))
        return api.get_raw_detailed_data(
            domain="graphext.com",
            output_format="parquet"  # Using Polars with Parquet for best performance
        )
    
    @op
    def upload_to_s3(data, s3_resource):
        """Upload data to S3"""
        bucket = s3_resource.Bucket("my-rescuetime-data")
        
        # Create a filename with timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"graphext_time_{timestamp}.parquet"
        
        # Convert to bytes if needed
        if hasattr(data, 'write_parquet'):
            # This is a Polars DataFrame
            import io
            buffer = io.BytesIO()
            data.write_parquet(buffer)
            buffer.seek(0)
            parquet_bytes = buffer.read()
        else:
            # Already in proper format
            parquet_bytes = data
            
        # Upload to S3
        bucket.put_object(
            Key=filename,
            Body=parquet_bytes,
            ContentType="application/octet-stream"
        )
        
        return f"s3://my-rescuetime-data/{filename}"
    
    @graph
    def rescuetime_etl():
        """Pipeline to extract RescueTime data and load to S3"""
        data = fetch_rescuetime_data()
        upload_to_s3(data)
    
    # Schedule to run every hour
    rescuetime_job = rescuetime_etl.to_job(
        schedule=ScheduleDefinition(cron_schedule="0 * * * *")
    )
    '''

if __name__ == "__main__":
    main() 