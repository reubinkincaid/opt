# dashboard.py - Creates the summary dashboards

import numpy as np
from utils import SECTOR_MAP

def create_overnight_dashboard(ticker_summary, output_file=None):
    """
    Creates a dashboard of overnight changes in sentiment
    
    Args:
        ticker_summary (DataFrame): Summary of ticker sentiment changes
        output_file (str, optional): File path to save dashboard output
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

def create_daily_dashboard(ticker_summary, output_file=None):
    """
    Creates a dashboard of day-to-day changes in sentiment
    
    Args:
        ticker_summary (DataFrame): Summary of ticker sentiment changes
        output_file (str, optional): File path to save dashboard output
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