# sentiment_analysis.py - Overnight and daily sentiment comparisons

import pandas as pd
import numpy as np

def analyze_overnight_changes(evening_df, morning_df):
    """
    Compare evening vs morning data to detect overnight changes in sentiment
    
    Args:
        evening_df (DataFrame): Evening options data
        morning_df (DataFrame): Morning options data
        
    Returns:
        tuple: (merged_data, ticker_summary, volume_factor)
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

def analyze_daily_changes(previous_day_df, current_day_df):
    """
    Compare previous day's close vs current day's close data to detect day-to-day changes in sentiment
    
    Args:
        previous_day_df (DataFrame): Previous day's options data
        current_day_df (DataFrame): Current day's options data
        
    Returns:
        tuple: (merged_data, ticker_summary, volume_factor)
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