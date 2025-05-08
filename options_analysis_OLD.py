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

# Combined list of tickers to monitor
DEFAULT_TICKERS = [
    # Major indices ETFs
    'SPY', 'QQQ', 'IWM', 'DIA', 'TQQQ', 'TNA',
    
    # Sector ETFs
    'XLF', 'XLK', 'XLE', 'XLU', 'XLV', 'XLP', 'XLI', 'XLB', 'XLY', 'XLRE', 'XBI', 'IBB',
    
    # Technology
    'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'NVDA', 'AMD', 'TSM', 'ADBE', 'NOW', 'PLTR', 
    'MU', 'RBLX', 'U', 'ROKU', 'RDDT', 'NBIS',
    
    # Finance
    'JPM', 'BAC', 'GS', 'V', 'MA', 
    
    # Energy & EV
    'TSLA', 'FSLR', 'ENVX', 'CSIQ', 'MBLY',
    
    # Crypto related
    'MARA', 'MSTR', 'RIOT',
    
    # Healthcare & Biotech
    'MRNA', 'NVAX', 'SGMO', 'SRPT', 'TCMD', 'VKTX', 'WGS', 'IMRX', 'BIVI',
    
    # Other stocks
    'COST', 'CPRT', 'DIS', 'PM', 'SPHR', 'SGI', 'CAVA', 'CVNA', 'GRAB', 'PDYN', 'SERV',
    'ACHR', 'JOBY'
]

# Sector mapping for dashboard
SECTOR_MAP = {
    # Indices
    'SPY': 'Index', 'QQQ': 'Index', 'IWM': 'Index', 'DIA': 'Index', 'TQQQ': 'Leveraged Index', 'TNA': 'Leveraged Index',
    
    # Sector ETFs
    'XLF': 'Financials', 'XLK': 'Technology', 'XLE': 'Energy', 'XLU': 'Utilities', 
    'XLV': 'Healthcare', 'XLP': 'Consumer Staples', 'XLI': 'Industrials', 
    'XLB': 'Materials', 'XLY': 'Consumer Discretionary', 'XLRE': 'Real Estate',
    'XBI': 'Biotech', 'IBB': 'Biotech',
    
    # Technology
    'AAPL': 'Technology', 'MSFT': 'Technology', 'GOOGL': 'Technology', 'META': 'Technology', 
    'NVDA': 'Semiconductor', 'AMD': 'Semiconductor', 'TSM': 'Semiconductor', 'MU': 'Semiconductor',
    'ADBE': 'Software', 'NOW': 'Software', 'PLTR': 'Software', 'RBLX': 'Software', 'U': 'Software',
    'ROKU': 'Technology', 'RDDT': 'Social Media', 'NBIS': 'Technology',
    
    # Finance
    'JPM': 'Financials', 'BAC': 'Financials', 'GS': 'Financials', 'V': 'Financials', 'MA': 'Financials',
    
    # Energy & EV
    'TSLA': 'EV', 'FSLR': 'Clean Energy', 'ENVX': 'Clean Energy', 'CSIQ': 'Clean Energy',
    'MBLY': 'Automotive Tech',
    
    # Crypto related
    'MARA': 'Crypto', 'MSTR': 'Crypto', 'RIOT': 'Crypto',
    
    # Healthcare & Biotech
    'MRNA': 'Biotech', 'NVAX': 'Biotech', 'SGMO': 'Biotech', 'SRPT': 'Biotech', 
    'TCMD': 'Healthcare', 'VKTX': 'Biotech', 'WGS': 'Biotech', 'IMRX': 'Biotech', 'BIVI': 'Biotech',
    
    # Other
    'COST': 'Retail', 'CPRT': 'Auto Services', 'DIS': 'Entertainment', 'PM': 'Consumer Goods',
    'SPHR': 'Technology', 'SGI': 'Technology', 'CAVA': 'Restaurant', 'CVNA': 'Auto Retail',
    'GRAB': 'Rideshare', 'PDYN': 'Technology', 'SERV': 'Services',
    'ACHR': 'eVTOL', 'JOBY': 'eVTOL',
    
    # Default for any missing tickers
    'DEFAULT': 'Other'
}

def process_ticker(ticker, index=None, total=None, run_gamma=True, run_vol=True):
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

def analyze_overnight_changes(evening_df, morning_df):
    """
    Compare evening vs morning data to detect overnight changes in sentiment
    """
    # Ensure we're comparing the same options
    common_columns = ['ticker', 'strike', 'expiration', 'option_type']
    
    # Merge the dataframes
    merged = evening_df.merge(
        morning_df,
        on=common_columns,
        suffixes=('_evening', '_morning')
    )
    
    # Calculate changes in IV and pricing
    merged['iv_change'] = merged['impliedVolatility_morning'] - merged['impliedVolatility_evening']
    merged['iv_change_pct'] = (merged['iv_change'] / merged['impliedVolatility_evening']) * 100
    
    merged['price_change'] = merged['lastPrice_morning'] - merged['lastPrice_evening']
    merged['price_change_pct'] = (merged['price_change'] / merged['lastPrice_evening']) * 100
    
    # Calculate sentiment scores
    # For calls: IV and price increase = bullish, decrease = bearish
    # For puts: IV and price increase = bearish, decrease = bullish
    merged['sentiment_score'] = np.where(
        merged['option_type'] == 'call',
        merged['price_change_pct'],  # Call price up = bullish
        -merged['price_change_pct']  # Put price up = bearish
    )
    
    # Add volume-weighted component if morning data has volume
    if 'volume_morning' in merged.columns and merged['volume_morning'].sum() > 0:
        # Morning volume indicates activity - weight score by volume
        merged['weighted_score'] = merged['sentiment_score'] * merged['volume_morning']
        volume_factor = True
    else:
        # No volume data - use open interest from evening as weighting
        merged['weighted_score'] = merged['sentiment_score'] * merged['openInterest_evening']
        volume_factor = False
    
    # Summarize by ticker
    ticker_summary = merged.groupby('ticker').agg({
        'weighted_score': 'sum',
        'sentiment_score': 'mean',
        'iv_change': 'mean',
        'price_change_pct': 'mean',
        'openInterest_evening': 'sum',
    }).reset_index()
    
    # Normalize by open interest
    ticker_summary['normalized_score'] = ticker_summary['weighted_score'] / ticker_summary['openInterest_evening']
    ticker_summary['sentiment'] = np.where(ticker_summary['normalized_score'] > 0, 'BULLISH', 'BEARISH')
    
    # Sort from most bullish to most bearish
    ticker_summary = ticker_summary.sort_values('normalized_score', ascending=False)
    
    return merged, ticker_summary, volume_factor

def create_overnight_dashboard(ticker_summary, output_file=None):
    """
    Creates a dashboard of overnight changes in sentiment
    """
    # Add sector mapping
    ticker_summary['sector'] = ticker_summary['ticker'].map(
        lambda x: SECTOR_MAP.get(x, SECTOR_MAP.get('DEFAULT', 'Other'))
    )
    
    # Calculate sector aggregates
    sector_summary = ticker_summary.groupby('sector').agg({
        'normalized_score': 'mean',
        'openInterest_evening': 'sum',
        'iv_change': 'mean'
    }).reset_index()
    
    sector_summary['sentiment'] = np.where(sector_summary['normalized_score'] > 0, 'BULLISH', 'BEARISH')
    sector_summary = sector_summary.sort_values('normalized_score', ascending=False)
    
    # Print the dashboard
    print("\n===== OVERNIGHT SENTIMENT DASHBOARD =====")
    
    print("\nTop 5 BULLISH Tickers:")
    bullish = ticker_summary[ticker_summary['sentiment'] == 'BULLISH'].head(5)
    for _, row in bullish.iterrows():
        print(f"{row['ticker']}: {row['normalized_score']:.2f} (IV Δ: {row['iv_change']*100:.1f}%)")
    
    print("\nTop 5 BEARISH Tickers:")
    bearish = ticker_summary[ticker_summary['sentiment'] == 'BEARISH'].tail(5).iloc[::-1]
    for _, row in bearish.iterrows():
        print(f"{row['ticker']}: {row['normalized_score']:.2f} (IV Δ: {row['iv_change']*100:.1f}%)")
    
    print("\nSector Sentiment:")
    for _, row in sector_summary.iterrows():
        print(f"{row['sector']}: {row['sentiment']} ({row['normalized_score']:.2f})")
    
    # Calculate overall market sentiment
    market_score = (ticker_summary['normalized_score'] * ticker_summary['openInterest_evening']).sum() / ticker_summary['openInterest_evening'].sum()
    market_direction = "BULLISH" if market_score > 0 else "BEARISH"
    
    print(f"\nOverall Market Sentiment: {market_direction} ({market_score:.2f})")
    
    # Save to file
    if output_file:
        with open(output_file, 'w') as f:
            f.write("OVERNIGHT SENTIMENT DASHBOARD\n\n")
            f.write(f"Overall Market: {market_direction} ({market_score:.2f})\n\n")
            
            f.write("Top Bullish:\n")
            for _, row in bullish.iterrows():
                f.write(f"{row['ticker']}: {row['normalized_score']:.2f}\n")
            
            f.write("\nTop Bearish:\n")
            for _, row in bearish.iterrows():
                f.write(f"{row['ticker']}: {row['normalized_score']:.2f}\n")
                
            f.write("\nSector Sentiment:\n")
            for _, row in sector_summary.iterrows():
                f.write(f"{row['sector']}: {row['sentiment']} ({row['normalized_score']:.2f})\n")
        
        print(f"Dashboard saved to {output_file}")

# output the sentiment analysis for both daily and overnight timeframes
def analyze_daily_changes(previous_day_df, current_day_df):
    """
    Compare previous day's close vs current day's close data to detect day-to-day changes in sentiment
    """
    # Ensure we're comparing the same options
    common_columns = ['ticker', 'strike', 'expiration', 'option_type']
    
    # Merge the dataframes
    merged = previous_day_df.merge(
        current_day_df,
        on=common_columns,
        suffixes=('_previous', '_current')
    )
    
    # Calculate changes in IV and pricing
    merged['iv_change'] = merged['impliedVolatility_current'] - merged['impliedVolatility_previous']
    merged['iv_change_pct'] = (merged['iv_change'] / merged['impliedVolatility_previous']) * 100
    
    merged['price_change'] = merged['lastPrice_current'] - merged['lastPrice_previous']
    merged['price_change_pct'] = (merged['price_change'] / merged['lastPrice_previous']) * 100
    
    # Calculate sentiment scores
    # For calls: IV and price increase = bullish, decrease = bearish
    # For puts: IV and price increase = bearish, decrease = bullish
    merged['sentiment_score'] = np.where(
        merged['option_type'] == 'call',
        merged['price_change_pct'],  # Call price up = bullish
        -merged['price_change_pct']  # Put price up = bearish
    )
    
    # Add volume-weighted component if current data has volume
    if 'volume_current' in merged.columns and merged['volume_current'].sum() > 0:
        # Current volume indicates activity - weight score by volume
        merged['weighted_score'] = merged['sentiment_score'] * merged['volume_current']
        volume_factor = True
    else:
        # No volume data - use open interest from previous as weighting
        merged['weighted_score'] = merged['sentiment_score'] * merged['openInterest_previous']
        volume_factor = False
    
    # Summarize by ticker
    ticker_summary = merged.groupby('ticker').agg({
        'weighted_score': 'sum',
        'sentiment_score': 'mean',
        'iv_change': 'mean',
        'price_change_pct': 'mean',
        'openInterest_previous': 'sum',
    }).reset_index()
    
    # Normalize by open interest
    ticker_summary['normalized_score'] = ticker_summary['weighted_score'] / ticker_summary['openInterest_previous']
    ticker_summary['sentiment'] = np.where(ticker_summary['normalized_score'] > 0, 'BULLISH', 'BEARISH')
    
    # Sort from most bullish to most bearish
    ticker_summary = ticker_summary.sort_values('normalized_score', ascending=False)
    
    return merged, ticker_summary, volume_factor

def create_daily_dashboard(ticker_summary, output_file=None):
    """
    Creates a dashboard of day-to-day changes in sentiment
    """
    # Add sector mapping
    ticker_summary['sector'] = ticker_summary['ticker'].map(
        lambda x: SECTOR_MAP.get(x, SECTOR_MAP.get('DEFAULT', 'Other'))
    )
    
    # Calculate sector aggregates
    sector_summary = ticker_summary.groupby('sector').agg({
        'normalized_score': 'mean',
        'openInterest_previous': 'sum',
        'iv_change': 'mean'
    }).reset_index()
    
    sector_summary['sentiment'] = np.where(sector_summary['normalized_score'] > 0, 'BULLISH', 'BEARISH')
    sector_summary = sector_summary.sort_values('normalized_score', ascending=False)
    
    # Print the dashboard
    print("\n===== DAILY SENTIMENT DASHBOARD =====")
    
    print("\nTop 5 BULLISH Tickers:")
    bullish = ticker_summary[ticker_summary['sentiment'] == 'BULLISH'].head(5)
    for _, row in bullish.iterrows():
        print(f"{row['ticker']}: {row['normalized_score']:.2f} (IV Δ: {row['iv_change']*100:.1f}%)")
    
    print("\nTop 5 BEARISH Tickers:")
    bearish = ticker_summary[ticker_summary['sentiment'] == 'BEARISH'].tail(5).iloc[::-1]
    for _, row in bearish.iterrows():
        print(f"{row['ticker']}: {row['normalized_score']:.2f} (IV Δ: {row['iv_change']*100:.1f}%)")
    
    print("\nSector Sentiment:")
    for _, row in sector_summary.iterrows():
        print(f"{row['sector']}: {row['sentiment']} ({row['normalized_score']:.2f})")
    
    # Calculate overall market sentiment
    market_score = (ticker_summary['normalized_score'] * ticker_summary['openInterest_previous']).sum() / ticker_summary['openInterest_previous'].sum()
    market_direction = "BULLISH" if market_score > 0 else "BEARISH"
    
    print(f"\nOverall Market Sentiment: {market_direction} ({market_score:.2f})")
    
    # Save to file if requested
    if output_file:
        with open(output_file, 'w') as f:
            f.write("DAILY SENTIMENT DASHBOARD\n\n")
            f.write(f"Overall Market: {market_direction} ({market_score:.2f})\n\n")
            
            f.write("Top Bullish:\n")
            for _, row in bullish.iterrows():
                f.write(f"{row['ticker']}: {row['normalized_score']:.2f}\n")
            
            f.write("\nTop Bearish:\n")
            for _, row in bearish.iterrows():
                f.write(f"{row['ticker']}: {row['normalized_score']:.2f}\n")
                
            f.write("\nSector Sentiment:\n")
            for _, row in sector_summary.iterrows():
                f.write(f"{row['sector']}: {row['sentiment']} ({row['normalized_score']:.2f})\n")
        
        print(f"Daily Dashboard saved to {output_file}")

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
            gamma_result, vol_surface_df = process_ticker(
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
    time_str = datetime.now().strftime('%H%M')
    
    # Output for gamma flip
    if gamma_results:
        output = ';'.join(gamma_results)
        gamma_file = f'options_data/tv_oi_string_{date_str}_{time_str}.txt'
        with open(gamma_file, 'w') as f:
            f.write(output)
        print(f"Gamma Flip Analysis saved to {gamma_file}")
    
    # Output for volatility surface
    if all_vol_surface_data:
        combined_df = pd.concat(all_vol_surface_data)
        combined_df = prepare_for_parquet(combined_df)
        
        # Save combined data
        filename = f'options_data/vol_surface_{date_str}_{time_str}_{run_type}.parquet'
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
                    dashboard_file = f'options_data/overnight_sentiment_dashboard_{date_str}_{time_str}.txt'
                    create_overnight_dashboard(summary, dashboard_file)
                    
                    # Save detailed analysis
                    merged_data.to_parquet(f'options_data/overnight_analysis_{date_str}_{time_str}.parquet')
                    summary.to_csv(f'options_data/overnight_sentiment_summary_{date_str}_{time_str}.csv')
                    
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
                    daily_dashboard_file = f'options_data/daily_sentiment_dashboard_{date_str}_{time_str}.txt'
                    create_daily_dashboard(daily_summary, daily_dashboard_file)
                    
                    # Save detailed analysis
                    daily_merged_data.to_parquet(f'options_data/daily_analysis_{date_str}_{time_str}.parquet')
                    daily_summary.to_csv(f'options_data/daily_sentiment_summary_{date_str}_{time_str}.csv')
                    
                except Exception as e:
                    print(f"Error comparing with previous day's evening data: {e}")
            else:
                print("No previous day's evening data found for comparison")
        
        print(f"Analyzing volatility skew...")
        skew_df = analyze_skew(combined_df)
        skew_file = f'options_data/skew_analysis_{date_str}_{time_str}.csv'
        skew_df.to_csv(skew_file)
        print(f"Skew Analysis saved to {skew_file}")
    
    # Report any failures
    if failed_tickers:
        print(f"\nFailed to fetch data for: {', '.join(failed_tickers)}")
    
    print(f"Automated run completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    run_automated_data_collection()