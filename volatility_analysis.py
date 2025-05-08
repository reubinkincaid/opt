# volatility_analysis.py - Volatility surface and skew analysis

import pandas as pd

def analyze_skew(df):
    """
    Analyze the volatility skew from options data
    
    Args:
        df (DataFrame): Dataframe containing options data
        
    Returns:
        DataFrame: Skew analysis results
    """
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