# data_collection.py - Handles fetching data from yfinance API

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime

from gamma_analysis import calculate_gamma_flip

def process_ticker(ticker, index=None, total=None, run_gamma=True, run_vol=True):
    """
    Process a single ticker to collect gamma flip and volatility surface data
    
    Args:
        ticker (str): The ticker symbol
        index (int): Current index for progress reporting
        total (int): Total tickers for progress reporting
        run_gamma (bool): Whether to run gamma flip analysis
        run_vol (bool): Whether to run volatility surface analysis
        
    Returns:
        tuple: (gamma_result, vol_surface_df)
    """
    print(f"Processing {ticker}" + (f" [{index}/{total}]" if index and total else ""))
    
    try:
        # Fetch data once from yfinance API
        data = yf.Ticker(ticker)
        
        # Get spot price
        hist = data.history(period="2d")
        if hist.empty:
            print(f"No price data for {ticker}")
            return None, None
            
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
            return None, None
            
        # === PART 1: Gamma Flip Calculation ===
        gamma_result = None
        if run_gamma:
            fromStrike, toStrike = 0.5 * spot, 2.0 * spot
            today = pd.Timestamp.today().date()
            all_options = []
            
            for exp in expiries:
                try:
                    opt = data.option_chain(exp)
                    calls, puts = opt.calls, opt.puts
                    
                    if not calls.empty and not puts.empty:
                        calls_copy = calls.copy()
                        puts_copy = puts.copy()
                        calls_copy['ExpirationDate'] = pd.to_datetime(exp)
                        puts_copy['ExpirationDate'] = pd.to_datetime(exp)
                        all_options.append((calls_copy, puts_copy))
                except Exception as e:
                    print(f"Error with gamma calc for {ticker} {exp}: {e}")
                    continue
            
            if all_options:
                gamma_result = calculate_gamma_flip(ticker, all_options, spot, fromStrike, toStrike, today)
        
        # === PART 2: Volatility Surface ===
        vol_surface_df = None
        if run_vol:
            vol_surface_data = []
            today_dt = datetime.now()
            
            for exp in expiries:
                try:
                    opt = data.option_chain(exp)
                    calls, puts = opt.calls, opt.puts
                    
                    exp_date = datetime.strptime(exp, '%Y-%m-%d')
                    dte = (exp_date - today_dt).days
                    
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
                vol_surface_df['date'] = datetime.now().strftime('%Y-%m-%d')
                vol_surface_df['timestamp'] = datetime.now().strftime('%H:%M:%S')
                vol_surface_df['underlying_price'] = price
                vol_surface_df['ticker'] = ticker
                
                # Add previous day close if available
                if prev_close is not None:
                    vol_surface_df['prev_close'] = prev_close
                    vol_surface_df['price_change_pct'] = (price - prev_close) / prev_close * 100
        
        return gamma_result, vol_surface_df
        
    except Exception as e:
        print(f"Error processing {ticker}: {e}")
        return None, None

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