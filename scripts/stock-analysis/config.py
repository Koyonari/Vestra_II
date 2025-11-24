import os
from dotenv import load_dotenv
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
env_path = project_root / '.env.local'

load_dotenv(env_path)

# Supabase Configuration
SUPABASE_URL = os.getenv('NEXT_PUBLIC_SUPABASE_URL')
SUPABASE_KEY = os.getenv('NEXT_PUBLIC_SUPABASE_ANON_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing required Supabase environment variables")

# Analysis Configuration
MAX_STOCKS = 100
DAYS_BACK = 90
PREDICTION_DAYS = 30
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '10'))

# Rate Limiting
REQUEST_DELAY_MIN = float(os.getenv('REQUEST_DELAY_MIN', '1.5'))
REQUEST_DELAY_MAX = float(os.getenv('REQUEST_DELAY_MAX', '3.0'))
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '5'))  # Process 5 stocks at a time
CHUNK_DELAY = float(os.getenv('CHUNK_DELAY', '5.0'))  # 5 second delay between chunks

# Fallback tickers (top companies) if web scraping fails
FALLBACK_TICKERS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'BRK-B', 'JPM', 'JNJ', 'V',
    'PG', 'XOM', 'MA', 'HD', 'CVX', 'BAC', 'ABBV', 'PFE', 'AVGO', 'COST',
    'DIS', 'KO', 'PEP', 'TMO', 'CSCO', 'ABT', 'MRK', 'ORCL', 'ADBE', 'CRM',
    'NKE', 'AMD', 'NFLX', 'CMCSA', 'INTC', 'VZ', 'QCOM', 'TMUS', 'PM', 'TXN',
    'UNH', 'WMT', 'CRM', 'LLY', 'INTU', 'ACN', 'DHR', 'NEE', 'BMY', 'RTX'
]

# User Agents for Web Scraping
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
]

# Finance-specific lexicon for sentiment analysis
FINANCE_LEXICON = {
    'bullish': 4.0, 'bearish': -3.0,
    'outperform': 3.0, 'underperform': -2.0,
    'buy': 3.0, 'sell': -2.0,
    'upgrade': 3.5, 'downgrade': -2.5,
    'beat': 3.0, 'miss': -2.0,
    'exceeded': 3.0, 'fell short': -2.0,
    'growth': 2.5, 'decline': -1.5,
    'profit': 2.5, 'loss': -1.5,
    'positive': 2.0, 'negative': -1.0,
    'strong': 2.0, 'weak': -1.0,
    'surge': 3.0, 'plunge': -2.0,
    'rise': 2.0, 'fall': -1.0,
    'rally': 3.0, 'crash': -2.5,
    'breakthrough': 3.0, 'breakdown': -2.0,
}

# Fallback stock tickers
FALLBACK_TICKERS = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA", "BRK-B", "JPM", "V",
    "JNJ", "UNH", "PG", "MA", "HD", "BAC", "XOM", "AVGO", "CVX", "COST",
    "ABBV", "MRK", "PEP", "KO", "LLY", "TMO", "CSCO", "ABT", "CRM", "MCD",
    "ACN", "WMT", "NKE", "DHR", "TXN", "UPS", "NEE", "PM", "ORCL", "IBM",
    "QCOM", "INTC", "NFLX", "ADBE", "AMD", "CMCSA", "HON", "PFE", "CAT", "UNP"
]
