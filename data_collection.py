# data_collection.py - Handles fetching data from yfinance API

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime
import time
import random
from functools import wraps

from gamma_analysis import calculate_gamma_flip

# Add the retry decorator
def retry_with_backoff(retries=5, backoff_factor=0.5, errors=(Exception,)):
    """
    Retry decorator with exponential backoff
    
    Args:
        retries: Number of times to retry
        backoff_factor: How much to backoff (exponentially)
        errors: Tuple of exceptions to catch
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            x = 0
            while True:
                try:
                    return f(*args, **kwargs)
                except errors as e:
                    if x == retries:
                        raise
                    
                    # Calculate sleep time with jitter
                    sleep_time = backoff_factor * (2 ** x) + random.uniform(0, 0.5)
                    print(f"Retry {x+1}/{retries} after error: {str(e)}. Sleeping for {sleep_time:.2f}s")
                    time.sleep(sleep_time)
                    x += 1
        return wrapper
    return decorator

# Helper function with retry for fetching ticker data
@retry_with_backoff(retries=3, backoff_factor=1, errors=(Exception,))
def fetch_ticker_data(ticker):
    """
    Fetch ticker data with retry logic
    
    Args:
        ticker (str): The ticker symbol
        
    Returns:
        yfinance.Ticker: Ticker data object
    """
    return yf.Ticker(ticker)

@retry_with_backoff(retries=3, backoff_factor=1, errors=(Exception,))
def fetch_ticker_history(ticker_obj, period="2d"):
    """
    Fetch ticker price history with retry logic
    
    Args:
        ticker_obj: yfinance.Ticker object
        period (str): Period to fetch
        
    Returns:
        DataFrame: Price history
    """
    return ticker_obj.history(period=period)

@retry_with_backoff(retries=3, backoff_factor=1, errors=(Exception,))
def fetch_option_chain(ticker_obj, expiry):
    """
    Fetch option chain for a specific expiry with retry logic
    
    Args:
        ticker_obj: yfinance.Ticker object
        expiry (str): Option expiry date
        
    Returns:
        tuple: (calls, puts) DataFrames
    """
    opt = ticker_obj.option_chain(expiry)
    return opt.calls, opt.puts

# Now modify the process_ticker function to use these retry-enabled functions
def process_ticker(ticker, index=None, total=None, run_gamma=True, run_vol=True, collect_raw=True, trading_date=None):
    """
    Process a single ticker to collect gamma flip and volatility surface data
    
    Args:
        ticker (str): The ticker symbol
        index (int): Current index for progress reporting
        total (int): Total tickers for progress reporting
        run_gamma (bool): Whether to run gamma flip analysis
        run_vol (bool): Whether to run volatility surface analysis
        collect_raw (bool): Whether to collect and return raw options data
        trading_date (datetime, optional): Trading session date
        
    Returns:
        tuple: (gamma_result, vol_surface_df, raw_data)
    """
    print(f"Processing {ticker}" + (f" [{index}/{total}]" if index and total else ""))
    
    # If trading_date is not provided, use current date
    if trading_date is None:
        trading_date = datetime.now()
    
    try:
        # Fetch data once from yfinance API with retry
        data = fetch_ticker_data(ticker)
        
        # Get spot price with retry
        hist = fetch_ticker_history(data, period="2d")
        if hist.empty:
            print(f"No price data for {ticker}")
            return None, None, None
            
        spot = hist['Close'].iloc[-1]
        price = spot
        
        # Get previous day close if available
        prev_close = None
        if len(hist) > 1:
            prev_close = hist['Close'].iloc[-2]
        
        # Get options data
        expiries = data.options
        if not expiries:
            print(f"No options data for {ticker}")
            return None, None, None
        
        # Raw data collection
        raw_data = None
        if collect_raw:
            raw_data = {ticker: {}}
            
        # === PART 1: Gamma Flip Calculation ===
        gamma_result = None
        all_options = []
        
        for exp in expiries:
            try:
                # Fetch option chain with retry
                calls, puts = fetch_option_chain(data, exp)
                
                # Store raw data if requested
                if collect_raw:
                    raw_data[ticker][exp] = {
                        'calls': calls.copy(),
                        'puts': puts.copy(),
                        'spot': spot,
                        'prev_close': prev_close,
                        'trading_date': trading_date.strftime('%Y-%m-%d')  # Add trading date
                    }
                
                if not calls.empty and not puts.empty:
                    calls_copy = calls.copy()
                    puts_copy = puts.copy()
                    calls_copy['ExpirationDate'] = pd.to_datetime(exp)
                    puts_copy['ExpirationDate'] = pd.to_datetime(exp)
                    all_options.append((calls_copy, puts_copy))
            except Exception as e:
                print(f"Error with gamma calc for {ticker} {exp}: {e}")
                continue
        
        if run_gamma and all_options:
            fromStrike, toStrike = 0.5 * spot, 2.0 * spot
            today = pd.Timestamp.today().date()
            gamma_result = calculate_gamma_flip(ticker, all_options, spot, fromStrike, toStrike, today)
        
        # === PART 2: Volatility Surface ===
        vol_surface_df = None
        if run_vol:
            vol_surface_data = []
            
            for exp in expiries:
                try:
                    # We already fetched the option chain above, so reuse it from raw_data if available
                    if collect_raw and exp in raw_data[ticker]:
                        calls = raw_data[ticker][exp]['calls']
                        puts = raw_data[ticker][exp]['puts']
                    else:
                        # If not available, fetch it again with retry
                        calls, puts = fetch_option_chain(data, exp)
                    
                    exp_date = datetime.strptime(exp, '%Y-%m-%d')
                    dte = (exp_date - trading_date).days
                    
                    if dte < 0:
                        continue
                    
                    if not calls.empty:
                        calls_vs = calls.copy()
                        calls_vs['option_type'] = 'call'
                        calls_vs['expiration'] = exp
                        calls_vs['dte'] = dte
                        calls_vs['moneyness'] = calls_vs['strike'] / price
                        vol_surface_data.append(calls_vs)
                    
                    if not puts.empty:
                        puts_vs = puts.copy()
                        puts_vs['option_type'] = 'put'
                        puts_vs['expiration'] = exp
                        puts_vs['dte'] = dte
                        puts_vs['moneyness'] = puts_vs['strike'] / price
                        vol_surface_data.append(puts_vs)
                    
                except Exception as e:
                    print(f"Error with vol surface for {ticker} {exp}: {e}")
                    continue
            
            if vol_surface_data:
                vol_surface_df = pd.concat(vol_surface_data)
                vol_surface_df['date'] = trading_date.strftime('%Y-%m-%d')
                vol_surface_df['timestamp'] = datetime.now().strftime('%H:%M:%S')
                vol_surface_df['trading_date'] = trading_date.strftime('%Y-%m-%d')
                vol_surface_df['underlying_price'] = price
                vol_surface_df['ticker'] = ticker
                
                # Add previous day close if available
                if prev_close is not None:
                    vol_surface_df['prev_close'] = prev_close
                    vol_surface_df['price_change_pct'] = (price - prev_close) / prev_close * 100
        
        return gamma_result, vol_surface_df, raw_data
        
    except Exception as e:
        print(f"Error processing {ticker}: {e}")
        return None, None, None

def prepare_for_parquet(df):
    """
    Fix dataframe columns for parquet compatibility
    
    Args:
        df (DataFrame): Raw dataframe
        
    Returns:
        DataFrame: Fixed dataframe ready for parquet
    """
    # Fix inTheMoney column - convert to proper boolean
    if 'inTheMoney' in df.columns:
        df['inTheMoney'] = df['inTheMoney'].astype(bool)
    
    # Handle any other problematic columns
    for col in df.columns:
        # Convert any object columns with numbers to float
        if df[col].dtype == 'object':
            try:
                df[col] = pd.to_numeric(df[col])
            except:
                # If conversion fails, keep the column as is
                pass
    
    return df

def save_raw_options_data(ticker_data, timestamp, run_type, trading_date=None):
    """
    Save raw options data before any processing
    
    Args:
        ticker_data (dict): Dictionary containing raw options data by ticker
        timestamp (str): Timestamp string for the filename
        run_type (str): 'morning' or 'evening'
        trading_date (datetime, optional): Trading session date
        
    Returns:
        str: Path to the saved file
    """
    # Create a DataFrame from the raw data
    raw_data = []
    
    # Use provided trading_date or current date
    if trading_date is None:
        trading_date = datetime.now()
        
    trading_date_str = trading_date.strftime('%Y-%m-%d')
    
    for ticker, data in ticker_data.items():
        for exp_date, options in data.items():
            if 'calls' in options and not options['calls'].empty:
                df_calls = options['calls'].copy()
                df_calls['ticker'] = ticker
                df_calls['expiration'] = exp_date
                df_calls['option_type'] = 'call'
                df_calls['timestamp'] = timestamp
                df_calls['run_type'] = run_type
                df_calls['trading_date'] = trading_date_str
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
                df_puts['trading_date'] = trading_date_str
                df_puts['underlying_price'] = options.get('spot', None)
                df_puts['prev_close'] = options.get('prev_close', None)
                raw_data.append(df_puts)
    
    # Combine all data and prepare for parquet
    if raw_data:
        combined_df = pd.concat(raw_data)
        combined_df = prepare_for_parquet(combined_df)
        
        # Save to parquet - use trading date in filename
        filepath = f'options_data/raw_options_{trading_date_str}_{run_type}.parquet'
        combined_df.to_parquet(filepath)
        return filepath
    
    return None