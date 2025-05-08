# utils.py - Common utilities, constants, and sector mappings

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