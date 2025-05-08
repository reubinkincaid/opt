# ©2025 reubinkincaid all rights reversed
# ♦♣♠♥ ╔══════════════════════════════════════╗ ♥♠♣♦
# ♣♠♥♦ ║     ●                 ●     GRIMACE  ║ ♦♥♠♣
# ♥♦♣♠ ║      ╲    _______    ╱               ║ ♠♣♥♦
# ♠♣♥♦ ║       ╲  │       │  ╱                ║ ♣♥♠♦
# ♦♥♣♠ ║        ╲ │  ◕ ◕  │ ╱                 ║ ♥♠♦♣
# ♣♠♥♦ ║         ╲│  ───  │╱                  ║ ♠♦♣♥
# ♥♦♣♠ ║          ╲ ┌───┐ ╱                   ║ ♦♣♥♠
# ♠♣♥♦ ║       ████████████                   ║ ♣♥♠♦
# ♦♥♣♠ ║     ██          ██                   ║ ♥♠♦♣
# ♣♠♥♦ ║    █   █████████   █                 ║ ♠♦♣♥
# ♥♦♣♠ ║    █   █       █   █                 ║ ♦♣♥♠
# ♠♣♥♦ ║    █   █████████   █                 ║ ♣♥♠♦
# ♦♥♣♠ ║     ██          ██                   ║ ♥♠♦♣
# ♣♠♥♦ ║       ████████████                   ║ ♠♦♣♥
# ♥♦♣♠ ╚══════════════════════════════════════╝ ♦♣♥♠

#!/usr/bin/env python3
# main.py - Entry point for the options analysis system

import os
import time
from datetime import datetime
import pandas as pd

# Import modules
from data_collection import process_ticker, prepare_for_parquet
from utils import DEFAULT_TICKERS
from gamma_analysis import calculate_gamma_flip
from volatility_analysis import analyze_skew
from sentiment_analysis import analyze_overnight_changes, analyze_daily_changes
from dashboard import create_overnight_dashboard, create_daily_dashboard

def run_automated_data_collection():
    """
    Automated pipeline for scheduled execution
    """
    # Determine if this is a morning or evening run based on current time
    current_hour = datetime.now().hour
    run_type = "evening" if (current_hour >= 20 or current_hour < 4) else "morning"
    
    print(f"Starting automated {run_type} run at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Fixed settings for automated runs
    batch_size = 10  # Process tickers in batches to avoid API issues
    delay = 2  # Delay between API calls in seconds
    
    # Always run both gamma and volatility analysis
    run_gamma = True
    run_vol = True
    
    # Result collections
    gamma_results = []
    all_vol_surface_data = []
    failed_tickers = []
    
    # Process tickers in batches
    for i in range(0, len(DEFAULT_TICKERS), batch_size):
        batch = DEFAULT_TICKERS[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(DEFAULT_TICKERS) + batch_size - 1)//batch_size}")
        
        for j, ticker in enumerate(batch, 1):
            start_time = time.time()
            
            # Add delay between API calls (except for the first one in each batch)
            if j > 1:
                time.sleep(delay)
            
            # Process ticker
            gamma_result, vol_surface_df, _ = process_ticker(
                ticker, 
                (i + j), 
                len(DEFAULT_TICKERS), 
                run_gamma=run_gamma,
                run_vol=run_vol
            )
            
            # Track results
            success = False
            
            # Handle gamma flip results
            if run_gamma:
                if gamma_result:
                    gamma_results.append(gamma_result)
                    success = True
            
            # Handle volatility surface results
            if run_vol:
                if vol_surface_df is not None:
                    all_vol_surface_data.append(vol_surface_df)
                    success = True
                else:
                    failed_tickers.append(ticker)
            
            # Reporting
            elapsed = time.time() - start_time
            if success:
                print(f"✓ {ticker} completed in {elapsed:.2f} seconds")
            else:
                print(f"✗ {ticker} failed")
    
    # Prepare directory
    os.makedirs('options_data', exist_ok=True)
    date_str = datetime.now().strftime('%Y-%m-%d')
    
    # Output for gamma flip
    if gamma_results:
        output = ';'.join(gamma_results)
        gamma_file = f'options_data/tradingview_{date_str}_{run_type}.txt'
        with open(gamma_file, 'w') as f:
            f.write(output)
        print(f"Gamma Flip Analysis saved to {gamma_file}")
    
    # Output for volatility surface
    if all_vol_surface_data:
        combined_df = pd.concat(all_vol_surface_data)
        combined_df = prepare_for_parquet(combined_df)
        
        # Save combined data
        filename = f'options_data/vol_surface_{date_str}_{run_type}.parquet'
        combined_df.to_parquet(filename)
        print(f"Volatility Surface data saved to {filename}")
        
        # For morning runs, try to find and compare with previous evening data (Overnight Analysis)
        if run_type == "morning":
            # Look for previous evening data file
            prev_date = date_str
            if current_hour < 12:  # If it's morning, look for yesterday's evening data
                prev_date = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            
            evening_files = [f for f in os.listdir('options_data') 
                           if f.startswith(f'vol_surface_{prev_date}') and 'evening' in f and f.endswith('.parquet')]
            
            if evening_files:
                latest_evening_file = sorted(evening_files)[-1]  # Get the most recent evening file
                print(f"Found previous evening data: {latest_evening_file}")
                
                try:
                    evening_df = pd.read_parquet(os.path.join('options_data', latest_evening_file))
                    
                    print("Analyzing overnight changes...")
                    merged_data, summary, volume_factor = analyze_overnight_changes(evening_df, combined_df)
                    
                    # Create and save dashboard
                    dashboard_file = f'options_data/overnight_sentiment_dashboard_{date_str}.txt'
                    create_overnight_dashboard(summary, dashboard_file)
                    
                    # Save detailed analysis
                    merged_data.to_parquet(f'options_data/overnight_analysis_{date_str}.parquet')
                    summary.to_csv(f'options_data/overnight_sentiment_summary_{date_str}.csv')
                    
                except Exception as e:
                    print(f"Error comparing with evening data: {e}")
            else:
                print("No evening data found for comparison")
        
        # For evening runs, try to find and compare with previous day's evening data (Daily Analysis)
        if run_type == "evening":
            # Look for previous day's evening data file
            prev_date = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            
            prev_evening_files = [f for f in os.listdir('options_data') 
                               if f.startswith(f'vol_surface_{prev_date}') and 'evening' in f and f.endswith('.parquet')]
            
            if prev_evening_files:
                latest_prev_evening_file = sorted(prev_evening_files)[-1]  # Get the most recent evening file from previous day
                print(f"Found previous day's evening data: {latest_prev_evening_file}")
                
                try:
                    prev_evening_df = pd.read_parquet(os.path.join('options_data', latest_prev_evening_file))
                    
                    print("Analyzing day-to-day changes...")
                    daily_merged_data, daily_summary, daily_volume_factor = analyze_daily_changes(prev_evening_df, combined_df)
                    
                    # Create and save dashboard
                    daily_dashboard_file = f'options_data/daily_sentiment_dashboard_{date_str}.txt'
                    create_daily_dashboard(daily_summary, daily_dashboard_file)
                    
                    # Save detailed analysis
                    daily_merged_data.to_parquet(f'options_data/daily_analysis_{date_str}.parquet')
                    daily_summary.to_csv(f'options_data/daily_sentiment_summary_{date_str}.csv')
                    
                except Exception as e:
                    print(f"Error comparing with previous day's evening data: {e}")
            else:
                print("No previous day's evening data found for comparison")
        
        print(f"Analyzing volatility skew...")
        skew_df = analyze_skew(combined_df)
        skew_file = f'options_data/skew_analysis_{date_str}_{run_type}.csv'
        skew_df.to_csv(skew_file)
        print(f"Skew Analysis saved to {skew_file}")
    
    # Report any failures
    if failed_tickers:
        print(f"\nFailed to fetch data for: {', '.join(failed_tickers)}")
    
    print(f"Automated run completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    run_automated_data_collection()