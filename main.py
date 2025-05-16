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
from data_collection import process_ticker, prepare_for_parquet, save_raw_options_data
from gamma_analysis import calculate_gamma_flip
from volatility_analysis import analyze_skew
from sentiment_analysis import analyze_overnight_changes, analyze_daily_changes, analyze_statistical_indicators
from dashboard import create_overnight_dashboard, create_daily_dashboard

# Dynamically import utils or test_utils depending on test_mode
def import_utils(test_mode=False):
    if test_mode:
        try:
            import test_utils as utils
            print("Using TEST_UTILS with reduced ticker list")
            return utils
        except ImportError:
            print("Warning: test_utils.py not found, falling back to standard utils.py")
            import utils
            return utils
    else:
        import utils
        return utils

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

def get_nested_folder_path(trading_date=None, run_type=None, base_dir='options_data', test_mode=False):
    """
    Create nested folder structure: year/month/week/day/run_type
    
    Args:
        trading_date (datetime, optional): Trading session date, defaults to result of get_trading_session_date()
        run_type (str, optional): 'morning' or 'evening', defaults to result of get_run_type()
        base_dir (str, optional): Base directory, defaults to 'options_data'
        test_mode (bool, optional): If True, use test directory instead
        
    Returns:
        dict: Dictionary with path and date information
    """
    if trading_date is None:
        trading_date = get_trading_session_date()
    elif isinstance(trading_date, str):
        trading_date = datetime.strptime(trading_date, '%Y-%m-%d')
        
    if run_type is None:
        run_type = get_run_type(trading_date.hour)
    
    # Apply test_mode modification to base_dir
    if test_mode:
        base_dir = base_dir.replace('options_data', 'options_data_test')
    
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

def run_automated_data_collection(test_mode=False):
    """
    Automated pipeline for scheduled execution
    
    Args:
        test_mode (bool): If True, save data to test folders instead of production
    """
    # Dynamically import the appropriate utils module
    utils = import_utils(test_mode)
    
    # Get the current time for logging purposes
    execution_time = datetime.now()
    
    # Determine the correct trading session date
    trading_date = get_trading_session_date()
    
    # Determine if this is a morning or evening run based on current time
    current_hour = execution_time.hour
    run_type = get_run_type(current_hour)
    
    # Get nested folder path using the trading session date
    folder_info = get_nested_folder_path(trading_date, run_type, test_mode=test_mode)
    folder_path = folder_info['path']
    date_str = folder_info['date_str']
    
    if test_mode:
        print(f"RUNNING IN TEST MODE - Data will be saved to: {folder_path}")
        print(f"Using {len(utils.DEFAULT_TICKERS)} tickers instead of full set")
    
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
    all_raw_data = {}
    price_data_list = []
    
    # Use the tickers from the dynamically imported utils module
    tickers_to_process = utils.DEFAULT_TICKERS
    
    # Process tickers in batches
    for i in range(0, len(tickers_to_process), batch_size):
        batch = tickers_to_process[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(tickers_to_process) + batch_size - 1)//batch_size}")
        
        for j, ticker in enumerate(batch, 1):
            start_time = time.time()
            
            # Add delay between API calls (except for the first one in each batch)
            if j > 1:
                time.sleep(delay)
            
            # Process ticker - now passes the utils module
            gamma_result, vol_surface_df, raw_data, ticker_price_data = process_ticker(
                ticker, 
                (i + j), 
                len(tickers_to_process), 
                run_gamma=run_gamma,
                run_vol=run_vol,
                utils_module=utils  # Pass the dynamically imported utils module
            )
            
            # Store raw data if available
            if raw_data:
                all_raw_data.update(raw_data)
            
            # Store price data if available
            if ticker_price_data:
                price_data_list.append(ticker_price_data)
            
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
                    vol_surface_df['date'] = trading_date.strftime('%Y-%m-%d')
                    vol_surface_df['trading_date'] = trading_date.strftime('%Y-%m-%d')
                    vol_surface_df['run_type'] = run_type
                    
                    all_vol_surface_data.append(vol_surface_df)
                    success = True
                else:
                    # Only add to failed tickers if it's not a statistical ticker
                    if ticker not in STATISTICAL_TICKERS:
                        failed_tickers.append(ticker)
            
            # Reporting
            elapsed = time.time() - start_time
            if success or ticker in STATISTICAL_TICKERS:
                print(f"✓ {ticker} completed in {elapsed:.2f} seconds")
            else:
                print(f"✗ {ticker} failed")
    
    # Save statistical_indicator price data in a single parquet
    if price_data_list:
        price_df = pd.DataFrame(price_data_list)
        
        # Create a dedicated folder for price data
        base_dir = 'options_data_test' if test_mode else 'options_data'
        price_data_dir = os.path.join(base_dir, folder_info['year'])
        os.makedirs(price_data_dir, exist_ok=True)
        
        # Path to the consolidated price data file
        consolidated_price_file = os.path.join(price_data_dir, 'price_data_history.parquet')
        
        # If historical file exists, read it and append new data
        if os.path.exists(consolidated_price_file):
            try:
                historical_price_df = pd.read_parquet(consolidated_price_file)
                
                # Check if we already have data for this trading date
                existing_data = historical_price_df[
                    (historical_price_df['trading_date'] == trading_date.strftime('%Y-%m-%d')) & 
                    (historical_price_df['run_type'] == run_type)
                ]
                
                if not existing_data.empty:
                    print(f"Found existing price data for {trading_date.strftime('%Y-%m-%d')} {run_type} - replacing")
                    # Remove existing data for this date/run_type
                    historical_price_df = historical_price_df.loc[
                        ~((historical_price_df['trading_date'] == trading_date.strftime('%Y-%m-%d')) & 
                        (historical_price_df['run_type'] == run_type))
                    ]
                
                # Add run_type to new data
                price_df['run_type'] = run_type
                
                # Append new data
                combined_price_df = pd.concat([historical_price_df, price_df])
                combined_price_df.to_parquet(consolidated_price_file)
                print(f"Updated historical price data saved to {consolidated_price_file}")
                
            except Exception as e:
                print(f"Error updating historical price data: {e}")
                # If error, just save today's data
                price_df['run_type'] = run_type
                price_df.to_parquet(consolidated_price_file)
                print(f"New historical price data saved to {consolidated_price_file}")
        else:
            # No historical file exists yet, create it
            price_df['run_type'] = run_type
            price_df.to_parquet(consolidated_price_file)
            print(f"New historical price data saved to {consolidated_price_file}")
        
        # Also save daily file in the nested structure for consistency
        daily_price_file = os.path.join(folder_path, 'price_data.parquet')
        price_df.to_parquet(daily_price_file)
        print(f"Daily price data saved to {daily_price_file}")
    
    # Save raw data
    if all_raw_data:
        raw_data_file = save_raw_options_data(
            all_raw_data, 
            trading_date.strftime('%Y-%m-%d_%H%M'), 
            run_type, 
            trading_date,
            test_mode=test_mode
        )
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
            prev_evening_info = get_nested_folder_path(prev_date, "evening", test_mode=test_mode)
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
            prev_evening_info = get_nested_folder_path(prev_date, "evening", test_mode=test_mode)
            prev_evening_path = prev_evening_info['path']
            prev_evening_file = os.path.join(prev_evening_path, 'vol_surface.parquet')
            prev_price_file = os.path.join(prev_evening_path, 'price_data.parquet')
            
            # Process options data for regular tickers
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
                    
                    # Store for later dashboard creation
                    options_summary = daily_summary
                    
                    # Save detailed analysis
                    daily_merged_file = os.path.join(folder_path, 'daily_analysis.parquet')
                    daily_merged_data.to_parquet(daily_merged_file)
                    
                    daily_summary_file = os.path.join(folder_path, 'daily_sentiment_summary.csv')
                    daily_summary.to_csv(daily_summary_file)
                    
                except Exception as e:
                    print(f"Error comparing with previous day's evening data: {e}")
                    options_summary = None
            else:
                print(f"No previous day's evening data found at: {prev_evening_file}")
                options_summary = None
            
            # Process price data for statistical indicators
            statistical_summary = None
            daily_price_file = os.path.join(folder_path, 'price_data.parquet')  # Current price file
            if os.path.exists(daily_price_file) and os.path.exists(prev_price_file):
                print(f"Found previous day's price data: {prev_price_file}")
                
                try:
                    prev_price_df = pd.read_parquet(prev_price_file)
                    curr_price_df = pd.read_parquet(daily_price_file)
                    
                    print("Analyzing statistical indicators...")
                    statistical_summary = analyze_statistical_indicators(
                        prev_price_df, curr_price_df, utils.STATISTICAL_TICKERS
                    )
                    
                    # Save statistical analysis
                    if statistical_summary is not None:
                        stat_file = os.path.join(folder_path, 'statistical_analysis.csv')
                        statistical_summary.to_csv(stat_file)
                        print(f"Statistical analysis saved to {stat_file}")
                    
                except Exception as e:
                    print(f"Error analyzing statistical indicators: {e}")
            
            # Create combined dashboard
            dashboard_file = os.path.join(folder_path, 'daily_sentiment_dashboard.txt')
            create_daily_dashboard(options_summary, dashboard_file, statistical_summary)
        
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
