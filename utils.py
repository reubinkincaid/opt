# utils.py - Common utilities, constants, and sector mappings

# TO ADD MORE TICKERS FOR OPTIONS DATA COLLECTION:
# In DEFAULT_TICKERS list:
#'XYZ',  # somewhere in the appropriate section
# In SECTOR_MAP dictionary:
#'XYZ': 'Technology',  # or whatever sector is appropriate

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
    'ACHR', 'JOBY',
    
    # Large Cap - Various Sectors
    'GOOG', 'BRK-B', 'JNJ', 'PG', 'UNH', 'HD', 'CVX', 'LLY', 'AVGO', 'MRK',
    'PEP', 'ABBV', 'ORCL', 'CSCO', 'ABT', 'CRM', 'MCD', 'ACN', 'NKE', 'TMO',
    'CMCSA', 'WMT', 'DHR', 'TXN', 'UPS', 'INTC', 'QCOM', 'IBM', 'INTU', 'LOW',
    'NEE', 'PLD', 'AXP', 'SCHW', 'MS', 'BLK', 'BMY', 'LIN', 'SPGI', 'HON',
    'UPS', 'PYPL', 'NFLX', 'ISRG', 'TGT', 'AMGN', 'MDT', 'SBUX', 'BKNG', 'COP',
    
    # Mid Cap
    'LULU', 'SNAP', 'SPOT', 'PINS', 'UAL', 'ETSY', 'CHWY', 'RIVN', 'CRWD', 'FTNT',
    'DDOG', 'AFRM', 'ON', 'STLD', 'CF', 'DAL', 'PANW', 'KHC', 'DASH', 'TEAM',
    'ZS', 'BILL', 'PTON', 'ENPH', 'TTD', 'DOCN', 'CROX', 'SWKS', 'JBLU', 'AAL',
    'CCL', 'NCLH', 'RCL', 'EXR', 'DKNG', 'CVNA', 'TRMB', 'IRM', 'CLX', 'K',
    
    # Small Cap
    'PTRA', 'KERN', 'CGC', 'SFIX', 'BYND', 'GPS', 'IRBT', 'UPST', 'BGFV', 'LAZR',
    'GME', 'AMC', 'BBBY', 'NKLA', 'WISH', 'PLBY', 'PRTY', 'APRN', 'FUBO', 'RIDE',
    'HOOD', 'XPEV', 'SKLZ', 'BLNK', 'EVGO', 'WKHS', 'PSFE', 'CLOV', 'ROOT', 'SPCE',
    'MNDY', 'FIGS', 'MAPS', 'OUST', 'DM', 'VRM', 'SDC', 'FVRR', 'OCGN', 'CRON',
    
    # International
    'BABA', 'NIO', 'VALE', 'BP', 'TM', 'RIO', 'BHP', 'GSK', 'PDD', 'SONY',
    'SAP', 'UL', 'DEO', 'BTI', 'NVS', 'HSBC', 'SAN', 'SHOP', 'SE', 'SHEL',
    
    # Industrials/Manufacturing
    'F', 'GM', 'CAT', 'DE', 'MMM', 'BA', 'LMT', 'RTX', 'UNP', 'NSC',
    
    # Telecom
    'VZ', 'T', 'TMUS', 'LUMN', 'ERIC',
    
    # Media/Entertainment
    'WBD', 'PARA', 'EA', 'ATVI', 'TTWO', 'LYV', 'IMAX',
    
    # Food/Restaurants
    'YUM', 'DNUT', 'CMG', 'DPZ', 'QSR', 'WEN', 'JACK', 'CAKE',
    
    # Software/Cloud
    'ZM', 'PATH', 'NET', 'SNOW', 'TWLO', 'MDB', 'OKTA', 'CFLT', 'AYX',

    # Statistical indicators (no options chains)
    'S5FI', 'S5TH'
]

# Tickers without options chains
STATISTICAL_TICKERS = ['S5FI', 'S5TH']

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
    'GOOG': 'Technology', 'CSCO': 'Technology', 'QCOM': 'Semiconductor', 'AVGO': 'Semiconductor',
    'INTC': 'Semiconductor', 'TXN': 'Semiconductor', 'ORCL': 'Software', 'CRM': 'Software',
    'INTU': 'Software', 'DDOG': 'Software', 'ZS': 'Software', 'NET': 'Software', 'SNOW': 'Software',
    'PATH': 'Software', 'TWLO': 'Software', 'MDB': 'Software', 'OKTA': 'Software', 'CFLT': 'Software',
    'AYX': 'Software', 'TTD': 'Software', 'ZM': 'Software', 'TEAM': 'Software', 'CRWD': 'Software',
    'FTNT': 'Software', 'PANW': 'Software', 'SNAP': 'Social Media', 'PINS': 'Social Media',
    
    # Finance
    'JPM': 'Financials', 'BAC': 'Financials', 'GS': 'Financials', 'V': 'Financials', 'MA': 'Financials',
    'AXP': 'Financials', 'SCHW': 'Financials', 'MS': 'Financials', 'BLK': 'Financials',
    'PYPL': 'Financials', 'HOOD': 'Financials', 'PSFE': 'Financials', 'AFRM': 'Financials',
    'HSBC': 'Financials', 'SAN': 'Financials',
    
    # Energy & EV
    'TSLA': 'EV', 'FSLR': 'Clean Energy', 'ENVX': 'Clean Energy', 'CSIQ': 'Clean Energy',
    'MBLY': 'Automotive Tech', 'RIVN': 'EV', 'BLNK': 'EV Infrastructure', 'EVGO': 'EV Infrastructure',
    'WKHS': 'EV', 'NKLA': 'EV', 'XPEV': 'EV', 'NIO': 'EV', 'LAZR': 'Automotive Tech',
    'CVX': 'Energy', 'COP': 'Energy', 'BP': 'Energy', 'SHEL': 'Energy', 'ENPH': 'Clean Energy',
    
    # Crypto related
    'MARA': 'Crypto', 'MSTR': 'Crypto', 'RIOT': 'Crypto', 'COIN': 'Crypto',
    
    # Healthcare & Biotech
    'MRNA': 'Biotech', 'NVAX': 'Biotech', 'SGMO': 'Biotech', 'SRPT': 'Biotech', 
    'TCMD': 'Healthcare', 'VKTX': 'Biotech', 'WGS': 'Biotech', 'IMRX': 'Biotech', 'BIVI': 'Biotech',
    'JNJ': 'Healthcare', 'UNH': 'Healthcare', 'LLY': 'Healthcare', 'MRK': 'Healthcare',
    'ABBV': 'Healthcare', 'ABT': 'Healthcare', 'BMY': 'Healthcare', 'AMGN': 'Biotech',
    'MDT': 'Healthcare', 'ISRG': 'Healthcare', 'CLOV': 'Healthcare', 'OCGN': 'Biotech',
    'CRON': 'Biotech', 'CGC': 'Biotech', 'BYND': 'Alternative Foods', 'GSK': 'Healthcare', 'NVS': 'Healthcare',
    
    # Retail & Consumer
    'COST': 'Retail', 'WMT': 'Retail', 'TGT': 'Retail', 'HD': 'Retail', 'LOW': 'Retail',
    'LULU': 'Retail', 'NKE': 'Retail', 'ETSY': 'E-Commerce', 'CHWY': 'E-Commerce',
    'SFIX': 'E-Commerce', 'WISH': 'E-Commerce', 'FVRR': 'E-Commerce', 'PG': 'Consumer Goods',
    'SBUX': 'Restaurant', 'MCD': 'Restaurant', 'CMG': 'Restaurant', 'DPZ': 'Restaurant',
    'YUM': 'Restaurant', 'QSR': 'Restaurant', 'WEN': 'Restaurant', 'JACK': 'Restaurant',
    'CAKE': 'Restaurant', 'DNUT': 'Restaurant', 'CAVA': 'Restaurant',
    'PEP': 'Consumer Goods', 'K': 'Consumer Goods', 'CLX': 'Consumer Goods',
    'PM': 'Consumer Goods', 'UL': 'Consumer Goods', 'DEO': 'Consumer Goods', 'BTI': 'Consumer Goods',
    
    # Transportation & Travel
    'UAL': 'Airlines', 'DAL': 'Airlines', 'JBLU': 'Airlines', 'AAL': 'Airlines',
    'ACHR': 'eVTOL', 'JOBY': 'eVTOL', 'CCL': 'Cruise Lines', 'NCLH': 'Cruise Lines', 'RCL': 'Cruise Lines',
    'UBER': 'Rideshare', 'LYFT': 'Rideshare', 'GRAB': 'Rideshare', 'DASH': 'Delivery',
    'CVNA': 'Auto Retail', 'VRM': 'Auto Retail', 'CPRT': 'Auto Services',
    
    # Industrials & Manufacturing
    'F': 'Auto Manufacturing', 'GM': 'Auto Manufacturing', 'TM': 'Auto Manufacturing',
    'CAT': 'Heavy Equipment', 'DE': 'Heavy Equipment',
    'MMM': 'Industrials', 'HON': 'Industrials', 'BA': 'Aerospace', 'LMT': 'Aerospace',
    'RTX': 'Aerospace', 'UNP': 'Rail', 'NSC': 'Rail',
    
    # Telecom & Media
    'VZ': 'Telecom', 'T': 'Telecom', 'TMUS': 'Telecom', 'LUMN': 'Telecom', 'ERIC': 'Telecom',
    'NFLX': 'Streaming', 'DIS': 'Entertainment', 'WBD': 'Entertainment', 'PARA': 'Entertainment',
    'EA': 'Gaming', 'ATVI': 'Gaming', 'TTWO': 'Gaming', 'RBLX': 'Gaming',
    'LYV': 'Entertainment', 'IMAX': 'Entertainment', 'SPOT': 'Streaming',
    
    # Real Estate
    'PLD': 'REITs', 'EXR': 'REITs', 'IRM': 'REITs',
    
    # Other
    'SPHR': 'Technology', 'SGI': 'Technology', 'PDYN': 'Technology', 'SERV': 'Services',
    'NEE': 'Utilities', 'LIN': 'Materials', 'SPGI': 'Business Services', 'BKNG': 'Travel',
    'BRK-B': 'Conglomerate', 'PTON': 'Fitness', 'GME': 'Retail', 'AMC': 'Entertainment',
    'BBBY': 'Retail', 'PLBY': 'Media', 'PRTY': 'Retail', 'APRN': 'Food Delivery',
    'FUBO': 'Streaming', 'RIDE': 'EV', 'DKNG': 'Sports Betting', 'SPCE': 'Space Tourism',
    'SKLZ': 'Gaming', 'MNDY': 'Software', 'FIGS': 'Healthcare Apparel', 'MAPS': 'Software',
    'OUST': 'Technology', 'DM': 'Manufacturing', 'SDC': 'Healthcare', 'CROX': 'Footwear',
    'SWKS': 'Semiconductor', 'TMO': 'Life Sciences', 'DHR': 'Life Sciences',
    'STLD': 'Steel', 'CF': 'Chemicals', 'ROOT': 'Insurance', 'ON': 'Semiconductor',
    'VALE': 'Mining', 'RIO': 'Mining', 'BHP': 'Mining',
    'PDD': 'E-Commerce', 'SONY': 'Electronics', 'BABA': 'E-Commerce', 'SHOP': 'E-Commerce', 'SE': 'E-Commerce',
    'BILL': 'Software', 'DOCN': 'Cloud Services',
    
    # Market breadth indicators
    'S5FI': 'Market Breadth',
    'S5TH': 'Market Breadth',

    # Default for any missing tickers
    'DEFAULT': 'Other'
}