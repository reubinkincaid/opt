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

import pandas as pd
import numpy as np
from scipy.stats import norm
import yfinance as yf
import time
import os
from datetime import datetime

def process_ticker(ticker, index=None, total=None, run_gamma=True, run_vol=True):
    print(f"Processing {ticker}" + (f" [{index}/{total}]" if index and total else ""))
    
    try:
        # Fetch data once from yfinance API
        data = yf.Ticker(ticker)
        
        # Get spot price
        hist = data.history(period="1d")
        if hist.empty:
            print(f"No price data for {ticker}")
            return None, None
            
        spot = hist['Close'].iloc[-1]
        price = spot
        
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
                vol_surface_df['underlying_price'] = price
                vol_surface_df['ticker'] = ticker
        
        return gamma_result, vol_surface_df
        
    except Exception as e:
        print(f"Error processing {ticker}: {e}")
        return None, None

def calculate_gamma_flip(ticker, all_options, spot, fromStrike, toStrike, today):
    try:
        # Prepare dataframes
        df_calls = pd.concat([opt[0] for opt in all_options], ignore_index=True)
        df_puts = pd.concat([opt[1] for opt in all_options], ignore_index=True)
        
        df_calls = df_calls[(df_calls['strike'] >= fromStrike) & (df_calls['strike'] <= toStrike)]
        df_puts = df_puts[(df_puts['strike'] >= fromStrike) & (df_puts['strike'] <= toStrike)]
        
        if df_calls.empty or df_puts.empty:
            print(f"No options in strike range for {ticker}")
            return None
        
        # Merge dataframes
        df = pd.merge(df_calls, df_puts, on=['ExpirationDate', 'strike'], suffixes=('_call', '_put'))
        if df.empty:
            print(f"Empty merged dataframe for {ticker}")
            return None
            
        df = df.rename(columns={'strike': 'StrikePrice'})
        
        # Calculate DTE
        df['daysTillExp'] = [1/252 if (np.busday_count(today, exp.date())) == 0 
                        else np.busday_count(today, exp.date())/252 for exp in df['ExpirationDate']]
        
        # Calculate gamma profile
        levels = np.linspace(fromStrike, toStrike, 30)
        totalGamma = []
        
        def calcGammaEx(S, K, vol, T, r, q, optType, OI):
            if T == 0 or vol == 0:
                return 0
            dp = (np.log(S/K) + (r - q + 0.5*vol**2)*T) / (vol*np.sqrt(T))
            gamma = np.exp(-q*T) * norm.pdf(dp) / (S * vol * np.sqrt(T))
            return OI * 100 * S * S * 0.01 * gamma
        
        for level in levels:
            df['callGEX'] = df.apply(lambda row: calcGammaEx(level, row['StrikePrice'], 
                                                        row['impliedVolatility_call'], 
                                                        row['daysTillExp'], 0, 0, 
                                                        "call", row['openInterest_call']), axis=1)
            df['putGEX'] = df.apply(lambda row: calcGammaEx(level, row['StrikePrice'], 
                                                        row['impliedVolatility_put'], 
                                                        row['daysTillExp'], 0, 0, 
                                                        "put", row['openInterest_put']), axis=1)
            totalGamma.append((df['callGEX'].sum() - df['putGEX'].sum()) / 10**9)
        
        # Find zero crossing
        zero_crossings = []
        for i in range(len(levels)-1):
            if (totalGamma[i] * totalGamma[i+1] <= 0):
                x0, x1 = levels[i], levels[i+1]
                y0, y1 = totalGamma[i], totalGamma[i+1]
                if y0 != y1:
                    gamma_flip = x0 - y0 * (x1 - x0) / (y1 - y0)
                    zero_crossings.append(gamma_flip)
        
        gamma_flip = zero_crossings[0] if zero_crossings else np.nan
        
        # Get top strikes
        strike_calls = df.groupby('StrikePrice')['openInterest_call'].sum().nlargest(2).index.tolist()
        strike_puts = df.groupby('StrikePrice')['openInterest_put'].sum().nlargest(2).index.tolist()
        
        # Format output
        output = f"{ticker}:{','.join(map(lambda x: f'{int(x)}', strike_calls))},{','.join(map(lambda x: f'{int(x)}', strike_puts))},{gamma_flip:.0f}"
        return output
    except Exception as e:
        print(f"Error in gamma flip calculation for {ticker}: {e}")
        return None

def analyze_skew(df):
    skew_data = {}
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker]
        
        for exp in ticker_df['expiration'].unique():
            exp_df = ticker_df[ticker_df['expiration'] == exp]
            calls = exp_df[exp_df['option_type'] == 'call']
            puts = exp_df[exp_df['option_type'] == 'put']
            
            if len(calls) > 3 and len(puts) > 3:
                otm_puts = puts[puts['moneyness'] < 0.95].sort_values('moneyness', ascending=False).head(3)
                otm_calls = calls[calls['moneyness'] > 1.05].sort_values('moneyness').head(3)
                
                if not otm_puts.empty and not otm_calls.empty:
                    put_avg_iv = otm_puts['impliedVolatility'].mean()
                    call_avg_iv = otm_calls['impliedVolatility'].mean()
                    skew = put_avg_iv - call_avg_iv
                    
                    key = f"{ticker}_{exp}"
                    skew_data[key] = {
                        'ticker': ticker,
                        'expiration': exp,
                        'dte': exp_df['dte'].iloc[0],
                        'put_iv': put_avg_iv,
                        'call_iv': call_avg_iv,
                        'skew': skew,
                        'skew_direction': 'Downside' if skew > 0 else 'Upside'
                    }
    
    return pd.DataFrame.from_dict(skew_data, orient='index')

def prepare_for_parquet(df):
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

def main():
    print("Select mode:")
    print("1. Gamma Flip Analysis Only")
    print("2. Volatility Surface Analysis Only")
    print("3. Both Analyses (default)")
    mode_choice = input("Enter choice (1/2/3): ").strip() or '3'
    
    tickers_input = input("Enter tickers (comma separated): ").strip().upper()
    tickers = [t.strip() for t in tickers_input.split(',')]
    
    if not tickers:
        print("No tickers provided. Exiting.")
        return
    
    print(f"Processing {len(tickers)} tickers...")
    
    # Determine whether to run each analysis type
    run_gamma = mode_choice in ['1', '3']
    run_vol = mode_choice in ['2', '3']
    
    # Configure API delay
    delay_input = input("Enter delay between API calls in seconds (default: 2): ").strip()
    delay = int(delay_input) if delay_input and delay_input.isdigit() else 2
    
    # Result collections
    gamma_results = []
    all_vol_surface_data = []
    failed_tickers = []
    
    for i, ticker in enumerate(tickers, 1):
        start_time = time.time()
        
        # Add delay between API calls (except for the first one)
        if i > 1:
            time.sleep(delay)
        
        # Process ticker
        gamma_result, vol_surface_df = process_ticker(
            ticker, 
            i, 
            len(tickers), 
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
    
    # Output for gamma flip
    if run_gamma and gamma_results:
        output = ';'.join(gamma_results)
        print("\nGamma Flip Analysis Output:")
        print(output)
    
    # Output for volatility surface
    if run_vol and all_vol_surface_data:
        combined_df = pd.concat(all_vol_surface_data)
        combined_df = prepare_for_parquet(combined_df)
        
        # Save combined data
        os.makedirs('options_data', exist_ok=True)
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        # Use ticker name in filename if only one ticker
        is_multiple_tickers = ',' in tickers_input
        if not is_multiple_tickers and tickers[0] not in failed_tickers:
            filename = f'options_data/vol_surface_{tickers[0]}_{date_str}.parquet'
        else:
            filename = f'options_data/vol_surface_combined_{date_str}.parquet'
        
        combined_df.to_parquet(filename)
        print(f"Volatility Surface data saved to {filename}")
        
        print(f"Analyzing volatility skew...")
        skew_df = analyze_skew(combined_df)
        print(f"Skew Analysis complete")
    
    # Report any failures
    if failed_tickers:
        print(f"\nFailed to fetch data for: {', '.join(failed_tickers)}")

if __name__ == "__main__":
    main()