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
from datetime import datetime, timedelta
import pandas as pd

# Import modules
from data_collection import process_ticker, prepare_for_parquet
from utils import DEFAULT_TICKERS
from gamma_analysis import calculate_gamma_flip
from volatility_analysis import analyze_skew
from sentiment_analysis import analyze_overnight_changes, analyze_daily_changes
from dashboard import create_overnight_dashboard, create_daily_dashboard

def get_trading_session_date():
    """
    Determine the correct trading session date regardless of when the job actually runs
    
    Returns:
        datetime: The trading session date
    """
    current_time = datetime.now()
    current_hour = current_time.hour
    
    # If running between midnight and 4 AM, this is still considered the previous day's evening session
    if 0 <= current_hour < 4:
        # Return yesterday's date
        return current_time - timedelta(days=1)
    
    return current_time

def get_run_type(current_hour=None):
    """
    Determine if this is a morning or evening run based on the hour
    
    Args:
        current_hour (int, optional): Hour to check, defaults to current hour
        
    Returns:
        str: 'morning' or 'evening'
    """
    if current_hour is None:
        current_hour = datetime.now().hour
        
    return "evening" if (current_hour >= 20 or current_hour < 4) else "morning"

def get_nested_folder_path(trading_date=None, run_type=None, base_dir='options_data'):
    """
    Create nested folder structure: year/month/week/day/run_type
    
    Args:
        trading_date (datetime, optional): Trading session date, defaults to result of get_trading_session_date()
        run_type (str, optional): 'morning' or 'evening', defaults to result of get_run_type()
        base_dir (str, optional): Base directory, defaults to 'options_data'
        
    Returns:
        dict: Dictionary with path and date information
    """
    if trading_date is None:
        trading_date = get_trading_session_date()
    elif isinstance(trading_date, str):
        trading_date = datetime.strptime(trading_date, '%Y-%m-%d')
        
    if run_type is None:
        run_type = get_run_type(trading_date.hour)
    
    # Extract date components
    year_folder = trading_date.strftime('%Y')
    month_folder = trading_date.strftime('%m')  # Just month number
    week_folder = f"W{trading_date.strftime('%W')}"  # Week number (00-53)
    day_folder = trading_date.strftime('%d')  # Just day number
    
    # Build the nested path
    nested_path = os.path.join(
        base_dir,
        year_folder,
        month_folder,
        week_folder,
        day_folder,
        run_type
    )
    
    # Create all directories in the path
    os.makedirs(nested_path, exist_ok=True)
    
    return {
        'path': nested_path,
        'year': year_folder,
        'month': month_folder,
        'week': week_folder,
        'day': day_folder,
        'run_type': run_type,
        'date_str': trading_date.strftime('%Y-%m-%d')  # Keep this for compatibility
    }

def save_raw_options_data(ticker_data, folder_path, trading_date):
    """
    Save raw options data before any processing
    
    Args:
        ticker_data (dict): Dictionary containing raw options data by ticker
        folder_path (str): Path to the folder where the file will be saved
        trading_date (datetime): Trading session date
        
    Returns:
        str: Path to the saved file
    """
    # Create a DataFrame from the raw data
    raw_data = []
    timestamp = trading_date.strftime('%Y-%m-%d')
    run_type = os.path.basename(folder_path)  # Extract from folder path
    
    for ticker, data in ticker_data.items():
        for exp_date, options in data.items():
            if 'calls' in options and not options['calls'].empty:
                df_calls = options['calls'].copy()
                df_calls['ticker'] = ticker
                df_calls['expiration'] = exp_date
                df_calls['option_type'] = 'call'
                df_calls['timestamp'] = timestamp
                df_calls['run_type'] = run_type
                df_calls['trading_date'] = timestamp  # Add the trading date explicitly
                df_calls['underlying_price'] = options.get('spot', None)
                df_calls['prev_close'] = options.get('prev_close', None)
                raw_data.append(df_calls)
            
            if 'puts' in options and not options['puts'].empty:
                df_puts = options['puts'].copy()
                df_puts['ticker'] = ticker
                df_puts['expiration'] = exp_date
                df_puts['option_type'] = 'put'
                df_puts['timestamp'] = timestamp
                df_puts['run_type'] = run_type
                df_puts['trading_date'] = timestamp  # Add the trading date explicitly
                df_puts['underlying_price'] = options.get('spot', None)
                df_puts['prev_close'] = options.get('prev_close', None)
                raw_data.append(df_puts)
    
    # Combine all data and prepare for parquet
    if raw_data:
        combined_df = pd.concat(raw_data)
        combined_df = prepare_for_parquet(combined_df)
        
        # Save to parquet with simplified name
        filepath = os.path.join(folder_path, 'raw_options.parquet')
        combined_df.to_parquet(filepath)
        return filepath
    
    return None

def run_automated_data_collection():
    """
    Automated pipeline for scheduled execution
    """
    # Get the current time for logging purposes
    execution_time = datetime.now()
    
    # Determine the correct trading session date
    trading_date = get_trading_session_date()
    
    # Determine if this is a morning or evening run based on current time
    current_hour = execution_time.hour
    run_type = get_run_type(current_hour)
    
    # Get nested folder path using the trading session date
    folder_info = get_nested_folder_path(trading_date, run_type)
    folder_path = folder_info['path']
    date_str = folder_info['date_str']
    
    print(f"Starting automated {run_type} run at {execution_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Trading session date: {date_str}")
    print(f"Saving data to: {folder_path}")
    
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
    
    # Add this line to collect raw data
    all_raw_data = {}
    
    # Process tickers in batches
    for i in range(0, len(DEFAULT_TICKERS), batch_size):
        batch = DEFAULT_TICKERS[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(DEFAULT_TICKERS) + batch_size - 1)//batch_size}")
        
        for j, ticker in enumerate(batch, 1):
            start_time = time.time()
            
            # Add delay between API calls (except for the first one in each batch)
            if j > 1:
                time.sleep(delay)
            
            # Process ticker - update to capture raw_data
            gamma_result, vol_surface_df, raw_data = process_ticker(
                ticker, 
                (i + j), 
                len(DEFAULT_TICKERS), 
                run_gamma=run_gamma,
                run_vol=run_vol
            )
            
            # Store raw data if available
            if raw_data:
                all_raw_data.update(raw_data)
            
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
                    # Update vol_surface_df with trading date instead of current date
                    vol_surface_df['date'] = trading_date.strftime('%Y-%m-%d')
                    vol_surface_df['trading_date'] = trading_date.strftime('%Y-%m-%d')
                    vol_surface_df['run_type'] = run_type
                    
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
    
    # Save raw data
    if all_raw_data:
        raw_data_file = save_raw_options_data(all_raw_data, folder_path, trading_date)
        print(f"Raw options data saved to {raw_data_file}")
        
    # Output for gamma flip
    if gamma_results:
        output = ';'.join(gamma_results)
        gamma_file = os.path.join(folder_path, 'gamma_flip.txt')
        with open(gamma_file, 'w') as f:
            f.write(output)
        print(f"Gamma Flip Analysis saved to {gamma_file}")
    
    # Output for volatility surface
    if all_vol_surface_data:
        combined_df = pd.concat(all_vol_surface_data)
        combined_df = prepare_for_parquet(combined_df)
        
        # Save combined data
        vol_surface_file = os.path.join(folder_path, 'vol_surface.parquet')
        combined_df.to_parquet(vol_surface_file)
        print(f"Volatility Surface data saved to {vol_surface_file}")
        
        # For morning runs, try to find and compare with previous evening data (Overnight Analysis)
        if run_type == "morning":
            # Determine previous evening's date
            prev_date = trading_date - timedelta(days=1)
            
            # Get path to previous evening data
            prev_evening_info = get_nested_folder_path(prev_date, "evening")
            prev_evening_path = prev_evening_info['path']
            prev_evening_file = os.path.join(prev_evening_path, 'vol_surface.parquet')
            
            if os.path.exists(prev_evening_file):
                print(f"Found previous evening data: {prev_evening_file}")
                
                try:
                    evening_df = pd.read_parquet(prev_evening_file)
                    
                    print("Analyzing overnight changes...")
                    merged_data, summary, volume_factor = analyze_overnight_changes(evening_df, combined_df)
                    
                    # Add trading date information to output
                    merged_data['trading_date'] = trading_date.strftime('%Y-%m-%d')
                    merged_data['prev_trading_date'] = prev_date.strftime('%Y-%m-%d')
                    summary['trading_date'] = trading_date.strftime('%Y-%m-%d')
                    summary['prev_trading_date'] = prev_date.strftime('%Y-%m-%d')
                    
                    # Create and save dashboard
                    dashboard_file = os.path.join(folder_path, 'overnight_sentiment_dashboard.txt')
                    create_overnight_dashboard(summary, dashboard_file)
                    
                    # Save detailed analysis
                    merged_data_file = os.path.join(folder_path, 'overnight_analysis.parquet')
                    merged_data.to_parquet(merged_data_file)
                    
                    summary_file = os.path.join(folder_path, 'overnight_sentiment_summary.csv')
                    summary.to_csv(summary_file)
                    
                except Exception as e:
                    print(f"Error comparing with evening data: {e}")
            else:
                print(f"No evening data found for comparison at: {prev_evening_file}")
        
        # For evening runs, try to find and compare with previous day's evening data (Daily Analysis)
        if run_type == "evening":
            # Look for previous day's evening data file
            prev_date = trading_date - timedelta(days=1)
            
            # Get path to previous day's evening data
            prev_evening_info = get_nested_folder_path(prev_date, "evening")
            prev_evening_path = prev_evening_info['path']
            prev_evening_file = os.path.join(prev_evening_path, 'vol_surface.parquet')
            
            if os.path.exists(prev_evening_file):
                print(f"Found previous day's evening data: {prev_evening_file}")
                
                try:
                    prev_evening_df = pd.read_parquet(prev_evening_file)
                    
                    print("Analyzing day-to-day changes...")
                    daily_merged_data, daily_summary, daily_volume_factor = analyze_daily_changes(prev_evening_df, combined_df)
                    
                    # Add trading date information to output
                    daily_merged_data['trading_date'] = trading_date.strftime('%Y-%m-%d')
                    daily_merged_data['prev_trading_date'] = prev_date.strftime('%Y-%m-%d')
                    daily_summary['trading_date'] = trading_date.strftime('%Y-%m-%d')
                    daily_summary['prev_trading_date'] = prev_date.strftime('%Y-%m-%d')
                    
                    # Create and save dashboard
                    daily_dashboard_file = os.path.join(folder_path, 'daily_sentiment_dashboard.txt')
                    create_daily_dashboard(daily_summary, daily_dashboard_file)
                    
                    # Save detailed analysis
                    daily_merged_file = os.path.join(folder_path, 'daily_analysis.parquet')
                    daily_merged_data.to_parquet(daily_merged_file)
                    
                    daily_summary_file = os.path.join(folder_path, 'daily_sentiment_summary.csv')
                    daily_summary.to_csv(daily_summary_file)
                    
                except Exception as e:
                    print(f"Error comparing with previous day's evening data: {e}")
            else:
                print(f"No previous day's evening data found at: {prev_evening_file}")
        
        print(f"Analyzing volatility skew...")
        skew_df = analyze_skew(combined_df)
        
        # Add trading date information to skew output
        skew_df['trading_date'] = trading_date.strftime('%Y-%m-%d')
        
        skew_file = os.path.join(folder_path, 'skew_analysis.csv')
        skew_df.to_csv(skew_file)
        print(f"Skew Analysis saved to {skew_file}")
    
    # Report any failures
    if failed_tickers:
        print(f"\nFailed to fetch data for: {', '.join(failed_tickers)}")
    
    # Use execution_time when logging completion time
    print(f"Automated run completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total execution time: {(datetime.now() - execution_time).total_seconds()} seconds")

if __name__ == "__main__":
    run_automated_data_collection()
