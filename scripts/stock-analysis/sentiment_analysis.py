import pandas as pd
import nltk
import time
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import datetime, timedelta
import os
from config import FINANCE_LEXICON, DAYS_BACK
from webscrape import scrape_finviz_news, scrape_yahoo_finance_news

# Setup NLTK
nltk_data_dir = os.path.join(os.path.expanduser('~'), 'nltk_data')
os.makedirs(nltk_data_dir, exist_ok=True)
nltk.data.path.append(nltk_data_dir)
nltk.download('vader_lexicon', quiet=True, download_dir=nltk_data_dir)


class SentimentAnalyzer:
    def __init__(self):
        self.sia = SentimentIntensityAnalyzer()
        # Add finance-specific terms to the lexicon
        self.sia.lexicon.update(FINANCE_LEXICON)
    
    def analyze_sentiment(self, text):
        """Analyze sentiment using VADER with finance-specific lexicon"""
        sentiment_scores = self.sia.polarity_scores(text)
        return sentiment_scores
    
    def categorize_sentiment(self, compound_score):
        """Categorize sentiment based on compound score"""
        if compound_score >= 0.05:
            return 'Bullish'
        elif compound_score <= -0.05:
            return 'Bearish'
        else:
            return 'Neutral'
    
    def analyze_ticker_sentiment(self, ticker_data, days_back=None):
        """Analyze sentiment for a specific ticker"""
        if days_back is None:
            days_back = DAYS_BACK
        
        ticker = ticker_data['ticker']
        name = ticker_data['name']
        
        print(f"Analyzing sentiment for {ticker} ({name})...")
        
        # Get news from multiple sources with retries
        attempts = 0
        max_attempts = 3
        sources_data = []
        
        while attempts < max_attempts and not sources_data:
            if attempts > 0:
                print(f"Retrying news fetch for {ticker} (attempt {attempts+1}/{max_attempts})...")
                time.sleep(5)  # Wait between retries
            
            # Try Finviz first
            finviz_df = scrape_finviz_news(ticker)
            if not finviz_df.empty:
                sources_data.append(finviz_df)
            
            # Try Yahoo Finance
            yahoo_df = scrape_yahoo_finance_news(ticker)
            if not yahoo_df.empty:
                sources_data.append(yahoo_df)
            
            attempts += 1
            
            if sources_data:  # If we got news from any source
                break
        
        # Combine available news
        news_df = pd.concat(sources_data, ignore_index=True) if sources_data else pd.DataFrame()
        
        if news_df.empty:
            print(f"No news found for {ticker} after {attempts} attempts")
            return self._default_neutral_sentiment(ticker, name)
        
        # Add sentiment analysis
        news_df['sentiment'] = news_df['headline'].apply(self.analyze_sentiment)
        news_df['compound'] = news_df['sentiment'].apply(lambda x: x['compound'])
        news_df['category'] = news_df['compound'].apply(self.categorize_sentiment)
        
        # Filter by date
        news_df['parsed_date'] = news_df.apply(self._parse_date, axis=1)
        cutoff_date = datetime.now() - timedelta(days=days_back)
        filtered_df = news_df[news_df['parsed_date'] >= cutoff_date]
        
        # If no recent news, use all available
        if filtered_df.empty and not news_df.empty:
            filtered_df = news_df
        
        # Calculate overall sentiment
        avg_sentiment = filtered_df['compound'].mean()
        sentiment_category = self.categorize_sentiment(avg_sentiment)
        sentiment_strength = abs(avg_sentiment)
        
        # Count sentiment categories
        sentiment_counts = filtered_df['category'].value_counts()
        
        # Calculate investment score (0-100)
        normalized_sentiment = (avg_sentiment + 1) / 2  # Scale from -1,1 to 0,1
        investment_score = 50 + (normalized_sentiment - 0.5) * 100
        
        # Apply sentiment strength as a confidence multiplier
        investment_score = min(100, max(0, investment_score * (1 + sentiment_strength * 0.5)))
        
        return {
            'ticker': ticker,
            'name': name,
            'avg_sentiment': round(avg_sentiment, 4),
            'sentiment_category': sentiment_category,
            'bullish_count': int(sentiment_counts.get('Bullish', 0)),
            'neutral_count': int(sentiment_counts.get('Neutral', 0)),
            'bearish_count': int(sentiment_counts.get('Bearish', 0)),
            'news_count': len(filtered_df),
            'sentiment_strength': round(sentiment_strength, 4),
            'investment_score': round(investment_score, 2),
            'news_details': filtered_df
        }
    
    def _parse_date(self, row):
        """Parse date from news row"""
        try:
            date_str = row['date']
            today = datetime.now()
            
            if not date_str or 'ago' in str(date_str).lower():
                return today
            
            if isinstance(date_str, str):
                if '/' in date_str:
                    parts = date_str.split('/')
                    if len(parts) == 3:
                        month, day, year = parts
                        if len(year) == 2:
                            year = '20' + year
                        return datetime(int(year), int(month), int(day))
                elif '-' in date_str:
                    try:
                        return datetime.strptime(date_str, '%b-%d-%y')
                    except:
                        try:
                            return datetime.strptime(date_str, '%Y-%m-%d')
                        except:
                            return today
                elif date_str.lower() == 'today':
                    return today
                elif date_str.lower() == 'yesterday':
                    return today - timedelta(days=1)
            
            return today
        
        except Exception as e:
            print(f"Date parsing error: {e} for date: {row['date']}")
            return datetime.now()
    
    def _default_neutral_sentiment(self, ticker, name):
        """Return default neutral sentiment when no news is available"""
        return {
            'ticker': ticker,
            'name': name,
            'avg_sentiment': 0.0,
            'sentiment_category': 'Neutral',
            'bullish_count': 0,
            'neutral_count': 1,
            'bearish_count': 0,
            'news_count': 0,
            'sentiment_strength': 0.0,
            'investment_score': 50.0
        }


if __name__ == "__main__":
    # Test sentiment analysis
    analyzer = SentimentAnalyzer()
    
    # Test with sample ticker
    test_data = {
        'ticker': 'AAPL',
        'name': 'Apple Inc.',
        'market_cap': 3000000000000,
        'sector': 'Technology'
    }
    
    result = analyzer.analyze_ticker_sentiment(test_data)
    print("\nSentiment Analysis Results:")
    print(f"Ticker: {result['ticker']}")
    print(f"Sentiment: {result['sentiment_category']} ({result['avg_sentiment']})")
    print(f"Investment Score: {result['investment_score']}")
    print(f"News Count: {result['news_count']}")
