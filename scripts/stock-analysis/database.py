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
            ticker = stock_data['ticker']
            print(f"  Upserting {ticker}...")
            
            # Prepare stock data matching existing schema
            stock = {
                'ticker': ticker,
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
            
            # Upsert stock data (ticker is primary key)
            self.supabase.table('stocks').upsert(stock, on_conflict='ticker').execute()
            print(f"    ✓ Stock data upserted")

            # Handle historical prices
            historical_count = 0
            if stock_data.get('historical_data') and len(stock_data['historical_data']) > 0:
                # Delete existing historical data
                self.supabase.table('stock_prices').delete().eq('ticker', ticker).execute()
                
                # Prepare historical data
                historical_data = [
                    {
                        'ticker': ticker,
                        'date': price['date'],
                        'price': float(price['price'])
                    }
                    for price in stock_data['historical_data']
                ]
                
                # Log date range
                if historical_data:
                    dates = [d['date'] for d in historical_data]
                    print(f"    Historical prices: {min(dates)} to {max(dates)}")
                
                # Insert new historical data in chunks
                chunk_size = 100
                for i in range(0, len(historical_data), chunk_size):
                    chunk = historical_data[i:i + chunk_size]
                    self.supabase.table('stock_prices').insert(chunk).execute()
                    historical_count += len(chunk)
                
                print(f"    ✓ Inserted {historical_count} historical prices")
            else:
                print(f"    ⚠ No historical data for {ticker}")

            # Handle predictions
            prediction_count = 0
            if stock_data.get('prediction') and stock_data['prediction'].get('data') and len(stock_data['prediction']['data']) > 0:
                # Delete existing predictions
                self.supabase.table('stock_predictions').delete().eq('ticker', ticker).execute()
                
                predictions = []
                pred_data_list = stock_data['prediction']['data']
                upper_bound_list = stock_data['prediction'].get('upper_bound', [])
                lower_bound_list = stock_data['prediction'].get('lower_bound', [])
                
                for i in range(len(pred_data_list)):
                    pred_data = pred_data_list[i]
                    upper = upper_bound_list[i] if i < len(upper_bound_list) else None
                    lower = lower_bound_list[i] if i < len(lower_bound_list) else None
                    
                    predictions.append({
                        'ticker': ticker,
                        'date': pred_data['date'],
                        'price': float(pred_data['price']),
                        'upper_bound': float(upper['price']) if upper and 'price' in upper else None,
                        'lower_bound': float(lower['price']) if lower and 'price' in lower else None
                    })
                
                # Log date range
                if predictions:
                    dates = [p['date'] for p in predictions]
                    print(f"    Predictions: {min(dates)} to {max(dates)}")
                
                # Insert new predictions in chunks
                chunk_size = 100
                for i in range(0, len(predictions), chunk_size):
                    chunk = predictions[i:i + chunk_size]
                    self.supabase.table('stock_predictions').insert(chunk).execute()
                    prediction_count += len(chunk)
                
                print(f"    ✓ Inserted {prediction_count} predictions")
            else:
                print(f"    ⚠ No prediction data for {ticker}")

            print(f"  ✓ {ticker} complete: {historical_count} prices, {prediction_count} predictions")
            return True

        except Exception as e:
            print(f"  ✗ Error upserting stock data for {stock_data.get('ticker', 'unknown')}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def write_analysis_to_database(self, ranked_stocks, shocking_predictions=None):
        """Write complete analysis results to database"""
        success_count = 0
        error_count = 0
        
        print(f"\nWriting {len(ranked_stocks)} stocks to database...")
        
        # Ensure unique ranks 1-N
        for rank, (idx, stock) in enumerate(ranked_stocks.iterrows(), start=1):
            try:
                # Prepare stock data
                stock_data = {
                    'ticker': stock['ticker'],
                    'name': stock['name'],
                    'sentiment_score': float(stock.get('avg_sentiment', 0)),
                    'sentiment_category': stock.get('sentiment_category', 'Neutral'),
                    'investment_score': float(stock.get('investment_score', 0)),
                    'news_count': int(stock.get('news_count', 0)),
                    'rank': rank,  # Sequential 1-based ranking starting from 1
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
                print(f"  ✗ Error processing {stock.get('ticker', 'unknown')}: {str(e)}")
                import traceback
                traceback.print_exc()
                error_count += 1
        
        print(f"\n{'='*70}")
        print(f"Database Write Summary:")
        print(f"{'='*70}")
        print(f"✓ Successfully wrote {success_count} stocks")
        print(f"✗ Failed to write {error_count} stocks")
        
        # Get some statistics from database
        try:
            stocks_result = self.supabase.table('stocks').select('ticker').execute()
            prices_result = self.supabase.table('stock_prices').select('ticker', count='exact').execute()
            predictions_result = self.supabase.table('stock_predictions').select('ticker', count='exact').execute()
            
            print(f"\nDatabase Statistics:")
            print(f"  Total stocks in DB: {len(stocks_result.data)}")
            print(f"  Total price records: {prices_result.count if hasattr(prices_result, 'count') else 'N/A'}")
            print(f"  Total prediction records: {predictions_result.count if hasattr(predictions_result, 'count') else 'N/A'}")
        except Exception as e:
            print(f"  Could not fetch database statistics: {e}")
        
        print(f"{'='*70}\n")
        
        return success_count, error_count
