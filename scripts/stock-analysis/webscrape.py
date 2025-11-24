import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import time
import random
from datetime import datetime, timedelta
from config import (
    USER_AGENTS, FALLBACK_TICKERS, REQUEST_DELAY_MIN, 
    REQUEST_DELAY_MAX, CHUNK_DELAY, CHUNK_SIZE
)


def get_random_user_agent():
    """Return a random user agent to avoid detection"""
    return random.choice(USER_AGENTS)


def get_top_101_stocks():
    """Get a comprehensive list of top stocks by combining multiple sources"""
    try:
        all_tickers = set()
        
        # Try S&P 500 components
        try:
            headers = {
                'User-Agent': get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
            }
            sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            sp500_response = requests.get(sp500_url, headers=headers)
            from io import StringIO
            sp500_table = pd.read_html(StringIO(sp500_response.text))
            sp500_df = sp500_table[0]
            all_tickers.update(sp500_df['Symbol'].str.strip().tolist())
        except Exception as e:
            print(f"Warning: Could not fetch S&P 500 data: {e}")
        
        # Add NASDAQ 100 components
        try:
            ndx_url = "https://en.wikipedia.org/wiki/Nasdaq-100"
            headers['User-Agent'] = get_random_user_agent()  # Rotate user agent
            ndx_response = requests.get(ndx_url, headers=headers)
            ndx_table = pd.read_html(StringIO(ndx_response.text))
            ndx_df = next((df for df in ndx_table if 'Ticker' in df.columns), None)
            if ndx_df is not None:
                all_tickers.update(ndx_df['Ticker'].str.strip().tolist())
        except Exception as e:
            print(f"Warning: Could not fetch NASDAQ 100 data: {e}")
        
        # Add Dow Jones components
        try:
            dow_url = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"
            headers['User-Agent'] = get_random_user_agent()  # Rotate user agent
            dow_response = requests.get(dow_url, headers=headers)
            dow_table = pd.read_html(StringIO(dow_response.text))
            dow_df = next((df for df in dow_table if 'Symbol' in df.columns), None)
            if dow_df is not None:
                all_tickers.update(dow_df['Symbol'].str.strip().tolist())
        except Exception as e:
            print(f"Warning: Could not fetch Dow Jones data: {e}")
            
        # Clean tickers and add fallback if needed
        all_tickers.update(FALLBACK_TICKERS)
        tickers = [t.strip().replace('.','-') for t in all_tickers if t and isinstance(t, str)]
        print(f"Fetching market cap data for {len(tickers)} stocks...")
        
        # Get market cap data in chunks
        results = []
        ticker_chunks = [tickers[i:i + CHUNK_SIZE] for i in range(0, len(tickers), CHUNK_SIZE)]
        
        for idx, chunk in enumerate(ticker_chunks):
            chunk_data = []
            for ticker in chunk:
                try:
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    market_cap = info.get('marketCap', 0)
                    name = info.get('shortName', ticker)
                    sector = info.get('sector', 'Unknown')
                    
                    chunk_data.append({
                        'ticker': ticker,
                        'name': name,
                        'market_cap': market_cap,
                        'sector': sector
                    })
                except Exception as e:
                    print(f"Error fetching data for {ticker}: {e}")
                
                # Rate limiting
                time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
            
            results.extend(chunk_data)
            success_count = len([r for r in chunk_data if r['market_cap'] > 0])
            print(f"Processed chunk {idx+1}/{len(ticker_chunks)} ({success_count} successful)")
            
            if idx < len(ticker_chunks) - 1:  # Don't sleep after last chunk
                time.sleep(CHUNK_DELAY)
        
        # Convert to DataFrame and sort by market cap
        stocks_df = pd.DataFrame(results)
        stocks_df = stocks_df[stocks_df['market_cap'] > 0]  # Filter out invalid entries
        stocks_df = stocks_df.sort_values('market_cap', ascending=False).reset_index(drop=True)
        
        print(f"\nFound {len(stocks_df)} valid stocks with market cap data")
        if len(stocks_df) < 100:
            print("Warning: Less than 100 stocks available for analysis")
        
        # Take top 100 by market cap
        top_stocks = stocks_df.head(100)
        return top_stocks
    
    except Exception as e:
        print(f"Error getting top stocks: {e}")
        return _get_fallback_stocks()


def _get_fallback_stocks():
    """Fallback method to get stock data using predefined list"""
    print("Using fallback stock list...")
    results = []
    
    for ticker in FALLBACK_TICKERS[:50]:
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            market_cap = info.get('marketCap', 0)
            name = info.get('shortName', ticker)
            sector = info.get('sector', 'Unknown')
            
            results.append({
                'ticker': ticker,
                'name': name,
                'market_cap': market_cap,
                'sector': sector
            })
        except Exception as e:
            print(f"Error in fallback list for {ticker}: {e}")
            results.append({
                'ticker': ticker,
                'name': ticker,
                'market_cap': 0,
                'sector': 'Unknown'
            })
        
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
    
    return pd.DataFrame(results)


def scrape_finviz_news(ticker):
    """Scrape news headlines for a specific ticker from Finviz"""
    url = f'https://finviz.com/quote.ashx?t={ticker}'
    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
        'Referer': 'https://finviz.com/'  # Add referrer
    }
    
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                print(f"Retry {attempt}/{max_retries} for {ticker}...")
                time.sleep(retry_delay)
            
            time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
            
            # First try to verify the ticker exists
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Check if we got a valid page
            if "is not found" in soup.text or "Error" in soup.title.text:
                print(f"Warning: {ticker} not found on Finviz")
                return pd.DataFrame()
            
            news_table = soup.find(id='news-table')
            if not news_table:
                print(f"Warning: No news table found for {ticker} on Finviz")
                continue  # Try again if we didn't get the news table
            
            news_data = []
            current_date = datetime.now().strftime('%m/%d/%y')
            
            for row in news_table.find_all('tr'):
                if not row.td:
                    continue
                
                try:
                    date_cell = row.td.text.strip().split() if row.td and row.td.text else []
                    
                    # Parse date and time
                    date_str = current_date
                    time_str = ''
                    
                    if len(date_cell) >= 1:
                        if ':' in date_cell[0]:  # Just time, use today's date
                            time_str = date_cell[0]
                        elif len(date_cell) >= 2:  # Date and time
                            date_str = date_cell[0]
                            time_str = date_cell[1] if ':' in date_cell[1] else ''
                    
                    headline = row.a.text.strip() if row.a else None
                    source = row.span.text.strip() if row.span else "Unknown"
                    
                    if headline and len(headline) > 5:  # Basic validation
                        news_data.append([date_str, time_str, headline, source])
                
                except Exception as e:
                    print(f"Warning: Error parsing news row for {ticker}: {e}")
                    continue
            
            if news_data:  # If we found any valid news
                return pd.DataFrame(news_data, columns=['date', 'time', 'headline', 'source'])
            
        except requests.exceptions.RequestException as e:
            print(f"Warning: Request failed for {ticker} on attempt {attempt+1}: {e}")
            if attempt == max_retries - 1:
                print(f"Error: Failed to scrape {ticker} news from Finviz after {max_retries} attempts")
                return pd.DataFrame()
        except Exception as e:
            print(f"Warning: Unexpected error for {ticker} on attempt {attempt+1}: {e}")
            if attempt == max_retries - 1:
                print(f"Error: Failed to process {ticker} news after {max_retries} attempts")
                return pd.DataFrame()
    
    return pd.DataFrame()


def scrape_yahoo_finance_news(ticker):
    """Scrape news from Yahoo Finance for a specific ticker"""
    base_url = f'https://finance.yahoo.com/quote/{ticker}'
    
    # Try different URLs if one fails
    urls = [
        f'{base_url}/news',
        base_url,  # Main quote page often has news
        f'{base_url}?p={ticker}'  # Alternative format
    ]
    
    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    }
    
    news_data = []
    for url in urls:
        try:
            time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
            
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Try different selectors for news items
            news_items = (
                soup.select('div[data-test="story"]') or  # New Yahoo Finance selector
                soup.select('li.js-stream-content') or
                soup.select('div.news-container') or
                soup.find_all('h3') or  # Generic h3 selector
                soup.find_all('a', {'data-test': 'quoteLink'})  # Alternative news links
            )
            
            if news_items:
                for item in news_items:
                    headline_elem = (
                        item.select_one('h3') or 
                        item.select_one('h4') or 
                        item.select_one('a') or 
                        item
                    )
                    
                    if headline_elem:
                        headline = headline_elem.text.strip()
                        if headline and len(headline) > 5:  # Basic validation
                            news_data.append([
                                datetime.now().strftime('%m/%d/%y'),
                                'N/A',
                                headline,
                                'Yahoo Finance'
                            ])
                
                if news_data:  # If we found news, no need to try other URLs
                    break
                    
        except requests.exceptions.RequestException as e:
            print(f"Warning: Failed to fetch {url} - {str(e)}")
            continue
        except Exception as e:
            print(f"Warning: Error processing {url} - {str(e)}")
            continue
    
    if not news_data:
        print(f"No Yahoo Finance news found for {ticker}")
        
    return pd.DataFrame(news_data, columns=['date', 'time', 'headline', 'source'])


def get_stock_price_data(ticker, days=90):
    """Get historical stock price data using yfinance"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)
        
        if hist.empty:
            print(f"No price data found for {ticker}")
            return None
        
        return hist
    
    except Exception as e:
        print(f"Error getting price data for {ticker}: {e}")
        return None


if __name__ == "__main__":
    # Test the functions
    print("Testing webscrape functions...")
    
    # Test get_top_101_stocks
    stocks = get_top_101_stocks()
    print(f"\nFound {len(stocks)} stocks")
    print(stocks.head())
    
    # Test scraping for AAPL
    if not stocks.empty:
        test_ticker = stocks.iloc[0]['ticker']
        print(f"\nTesting news scraping for {test_ticker}...")
        
        finviz_news = scrape_finviz_news(test_ticker)
        print(f"Finviz news items: {len(finviz_news)}")
        
        price_data = get_stock_price_data(test_ticker)
        print(f"Price data points: {len(price_data) if price_data is not None else 0}")
