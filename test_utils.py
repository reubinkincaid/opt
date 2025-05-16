# test_utils.py - Test version of utils.py with reduced ticker lists for faster testing

# Small subset of tickers for testing
DEFAULT_TICKERS = [
    # Major indices ETFs
    'SPY', 'QQQ',
    
    # Technology
    'AAPL', 'MSFT', 'NVDA',
    
    # Finance
    'JPM',
    
    # Energy & EV
    'TSLA',
    
    # Crypto related
    'MSTR',
    
    # Healthcare & Biotech
    'MRNA',
    
    # Other stocks
    'COST', 'DIS'
]

# List of tickers that failed in the previous run - keep the same as in utils.py
EXCLUDED_TICKERS = [
    'PTRA',  # Delisted
    'KERN',  # Delisted
    'BBBY',  # Bankrupt/delisted
]

# Statistical indicators (no options chains)
STATISTICAL_TICKERS = []

# Filter out excluded tickers from the default list
ACTIVE_TICKERS = [ticker for ticker in DEFAULT_TICKERS if ticker not in EXCLUDED_TICKERS]

# Sector mapping for dashboard - include only sectors for the test tickers
SECTOR_MAP = {
    # Indices
    'SPY': 'Index', 'QQQ': 'Index',
    
    # Technology
    'AAPL': 'Technology', 'MSFT': 'Technology', 'NVDA': 'Semiconductor',
    
    # Finance
    'JPM': 'Financials',
    
    # Energy & EV
    'TSLA': 'EV',
    
    # Crypto related
    'MSTR': 'Crypto',
    
    # Healthcare & Biotech
    'MRNA': 'Biotech',
    
    # Other
    'COST': 'Retail', 'DIS': 'Entertainment',
    
    # Default for any missing tickers
    'DEFAULT': 'Other'
}