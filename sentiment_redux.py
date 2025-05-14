# this is to be run in case of missing data

import os
import pandas as pd
from datetime import datetime, timedelta
from sentiment_analysis import analyze_overnight_changes, analyze_daily_changes
from dashboard import create_overnight_dashboard, create_daily_dashboard

# Define dates (today and yesterday)
today_date = datetime.now()
yesterday_date = today_date - timedelta(days=1)

# Define paths based on your folder structure
run_type = "evening"  # or "morning" depending on your needs
today_year, today_month = today_date.strftime('%Y'), today_date.strftime('%m')
today_week, today_day = f"W{today_date.strftime('%W')}", today_date.strftime('%d')
yesterday_year, yesterday_month = yesterday_date.strftime('%Y'), yesterday_date.strftime('%m')
yesterday_week, yesterday_day = f"W{yesterday_date.strftime('%W')}", yesterday_date.strftime('%d')

# Build paths
today_path = os.path.join('options_data', today_year, today_month, today_week, today_day, run_type)
yesterday_path = os.path.join('options_data', yesterday_year, yesterday_month, yesterday_week, yesterday_day, run_type)

# Ensure the output directory exists
os.makedirs(today_path, exist_ok=True)

# Define file paths
today_file = os.path.join(today_path, 'vol_surface.parquet')
yesterday_file = os.path.join(yesterday_path, 'vol_surface.parquet')

# Check if files exist
if not os.path.exists(today_file):
    print(f"Error: Today's data not found at {today_file}")
    exit(1)

if not os.path.exists(yesterday_file):
    print(f"Error: Yesterday's data not found at {yesterday_file}")
    # You could specify an alternative location here if needed
    exit(1)

# Load data
today_df = pd.read_parquet(today_file)
yesterday_df = pd.read_parquet(yesterday_file)

# Run sentiment analysis
print("Analyzing daily changes...")
merged_data, summary, _ = analyze_daily_changes(yesterday_df, today_df)

# Create and save dashboard
dashboard_file = os.path.join(today_path, 'daily_sentiment_dashboard.txt')
create_daily_dashboard(summary, dashboard_file)

# Save detailed analysis
merged_data.to_parquet(os.path.join(today_path, 'daily_analysis.parquet'))
summary.to_csv(os.path.join(today_path, 'daily_sentiment_summary.csv'))

print(f"Sentiment analysis completed. Results saved to {today_path}")