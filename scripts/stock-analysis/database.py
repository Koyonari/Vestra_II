from supabase import create_client, Client
from datetime import datetime
import pandas as pd
from config import SUPABASE_URL, SUPABASE_KEY


class DatabaseManager:
    def __init__(self):
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Missing Supabase credentials")
        
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    def upsert_stock_data(self, stock_data):
        """Insert or update complete stock data in Supabase"""
        try:
            # Prepare stock data matching existing schema
            stock = {
                'ticker': stock_data['ticker'],
                'name': stock_data['name'],
                'sentiment': {
                    'score': float(stock_data.get('sentiment_score', 0)),
                    'category': stock_data.get('sentiment_category', 'Neutral'),
                    'investment_score': float(stock_data.get('investment_score', 0))
                },
                'news_count': int(stock_data.get('news_count', 0)),
                'rank': int(stock_data.get('rank', 0)),
                'investment_score': float(stock_data.get('investment_score', 0)),
                'last_updated': datetime.now().isoformat()
            }
            
            # Delete existing data first (if any)
            self.supabase.table('stocks').delete().eq('id', stock['id']).execute()
            
            # Insert new stock data
            self.supabase.table('stocks').insert(stock).execute()

            # Handle historical prices
            if stock_data.get('historical_data'):
                # Prepare historical data
                historical_data = [
                    {
                        'ticker': stock_data['ticker'],
                        'date': price['date'],
                        'price': float(price['price'])
                    }
                    for price in stock_data['historical_data']
                ]
                
                # Delete existing historical data
                self.supabase.table('stock_prices').delete().eq('ticker', stock_data['ticker']).execute()
                
                # Insert new historical data
                if historical_data:
                    self.supabase.table('stock_prices').insert(historical_data).execute()

            # Handle predictions
            if stock_data.get('prediction') and stock_data['prediction'].get('data'):
                predictions = []
                for i in range(len(stock_data['prediction']['data'])):
                    pred_data = stock_data['prediction']['data'][i]
                    upper = stock_data['prediction']['upper_bound'][i] if stock_data['prediction'].get('upper_bound') else None
                    lower = stock_data['prediction']['lower_bound'][i] if stock_data['prediction'].get('lower_bound') else None
                    
                    predictions.append({
                        'ticker': stock_data['ticker'],
                        'date': pred_data['date'],
                        'price': float(pred_data['price']),
                        'upper_bound': float(upper['price']) if upper else None,
                        'lower_bound': float(lower['price']) if lower else None
                    })
                
                # Delete existing predictions
                self.supabase.table('stock_predictions').delete().eq('ticker', stock_data['ticker']).execute()
                
                # Insert new predictions
                if predictions:
                    self.supabase.table('stock_predictions').insert(predictions).execute()

            return True

        except Exception as e:
            print(f"Error upserting stock data for {stock_data.get('ticker', 'unknown')}: {e}")
            return False
            
    def _ensure_tables_exist(self):
        """Ensure all required tables exist with correct schema"""
        try:
            # Create stocks table
            self.supabase.table('stocks').select('*').limit(1).execute()
        except Exception as e:
            if 'does not exist' in str(e).lower():
                self._create_tables()
    
    def _create_tables(self):
        """Create database tables if they don't exist"""
        try:
            # Create stocks table
            self.supabase.query("""
                create table if not exists public.stocks (
                    ticker text not null,
                    name text,
                    sentiment_score float8,
                    sentiment_category text,
                    sentiment jsonb,
                    news_count integer,
                    rank integer,
                    investment_score float8,
                    last_updated timestamp with time zone,
                    is_public boolean default true,
                    constraint stocks_pkey primary key (ticker)
                );
            """).execute()

            # Create stock_prices table
            self.supabase.query("""
                create table if not exists public.stock_prices (
                    ticker text not null,
                    date date not null,
                    price float8,
                    is_public boolean default true,
                    constraint stock_prices_pkey primary key (ticker, date)
                );
            """).execute()

            # Create stock_predictions table
            self.supabase.query("""
                create table if not exists public.stock_predictions (
                    ticker text not null,
                    date date not null,
                    price float8,
                    upper_bound float8,
                    lower_bound float8,
                    is_public boolean default true,
                    constraint stock_predictions_pkey primary key (ticker, date)
                );
            """).execute()

            # Enable RLS and create policies
            policies = [
                "alter table stocks enable row level security;",
                "alter table stock_prices enable row level security;",
                "alter table stock_predictions enable row level security;",
                
                # Stocks policies
                """create policy "Public stocks are viewable by everyone"
                   on stocks for select using (is_public = true);""",
                """create policy "Authenticated users can insert public stocks"
                   on stocks for insert with check (is_public = true);""",
                """create policy "Authenticated users can update public stocks"
                   on stocks for update using (is_public = true)
                   with check (is_public = true);""",
                """create policy "Authenticated users can delete public stocks"
                   on stocks for delete using (is_public = true);""",
                
                # Similar policies for other tables...
            ]
            
            for policy in policies:
                try:
                    self.supabase.query(policy).execute()
                except Exception as e:
                    if 'already exists' not in str(e).lower():
                        print(f"Warning: Could not create policy: {e}")
        
        except Exception as e:
            print(f"Error creating tables: {e}")
            raise
    
    def write_analysis_to_database(self, ranked_stocks, shocking_predictions=None):
        """Write complete analysis results to database"""
        success_count = 0
        error_count = 0
        
        for idx, stock in ranked_stocks.iterrows():
            try:
                # Prepare stock data
                stock_data = {
                    'ticker': stock['ticker'],
                    'name': stock['name'],
                    'sentiment_score': float(stock.get('avg_sentiment', 0)),
                    'sentiment_category': stock.get('sentiment_category', 'Neutral'),
                    'investment_score': float(stock.get('investment_score', 0)),
                    'news_count': int(stock.get('news_count', 0)),
                    'rank': idx + 1,  # 1-based ranking
                    'historical_data': stock.get('historical_data', []),
                    'prediction': stock.get('prediction', {
                        'data': [],
                        'upper_bound': [],
                        'lower_bound': []
                    })
                }
                
                if self.upsert_stock_data(stock_data):
                    success_count += 1
                else:
                    error_count += 1
                    
            except Exception as e:
                print(f"Error processing {stock.get('ticker', 'unknown')}: {str(e)}")
                error_count += 1
                
        print(f"\nDatabase Write Summary:")
        print(f"✓ Successfully wrote {success_count} stocks")
        print(f"✗ Failed to write {error_count} stocks")
        
        return success_count, error_count
