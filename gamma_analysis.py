# gamma_analysis.py - Contains gamma flip calculations

import pandas as pd
import numpy as np
from scipy.stats import norm

def calculate_gamma_flip(ticker, all_options, spot, fromStrike, toStrike, today):
    """
    Calculate the gamma flip point and key strike prices for a ticker
    
    Args:
        ticker (str): The ticker symbol
        all_options (list): List of tuples of (calls, puts) dataframes
        spot (float): Current spot price
        fromStrike (float): Lower bound of strike prices to consider
        toStrike (float): Upper bound of strike prices to consider
        today (datetime): Current date
        
    Returns:
        str: Formatted string with ticker, key strikes, and gamma flip point
    """
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