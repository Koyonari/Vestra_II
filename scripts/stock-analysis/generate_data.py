import json
import os
from datetime import datetime
import pandas as pd
from webscrape import get_top_101_stocks, get_stock_price_data
from sentiment_analysis import SentimentAnalyzer
from predict import predict_stock_trend, generate_shocking_predictions
from config import MAX_STOCKS


def export_stock_data_to_json(ticker, name, price_data, prediction_result, sentiment_data):
    """Export stock data to JSON format matching database schema"""
    if price_data is None or prediction_result is None:
        return None
    
    try:
        # Convert price data to historical format
        historical_data = [
            {
                'date': date.strftime('%Y-%m-%d'),
                'price': float(price)
            }
            for date, price in price_data['Close'].items()
        ]

        # Format prediction data
        prediction_dates = pd.date_range(
            start=price_data.index[-1] + pd.Timedelta(days=1),
            periods=len(prediction_result['predictions'])
        )
        
        prediction_data = {
            'data': [
                {
                    'date': date.strftime('%Y-%m-%d'),
                    'price': float(price)
                }
                for date, price in zip(prediction_dates, prediction_result['predictions'])
            ],
            'upper_bound': [
                {
                    'date': date.strftime('%Y-%m-%d'),
                    'price': float(price)
                }
                for date, price in zip(prediction_dates, prediction_result['upper_bound'])
            ],
            'lower_bound': [
                {
                    'date': date.strftime('%Y-%m-%d'),
                    'price': float(price)
                }
                for date, price in zip(prediction_dates, prediction_result['lower_bound'])
            ]
        }
        
        # Create final stock data structure
        stock_data = {
            'ticker': ticker,
            'name': name,
            'sentiment': {
                'score': float(sentiment_data['compound']) if 'compound' in sentiment_data else float(sentiment_data.get('avg_sentiment', 0)),
                'category': sentiment_data['category'] if 'category' in sentiment_data else sentiment_data.get('sentiment_category', 'Neutral'),
                'investment_score': float(sentiment_data['investment_score'])
            },
            'historical_data': historical_data,
            'prediction': prediction_data,
            'last_updated': datetime.now().isoformat()
        }
        
        return stock_data
        
    except Exception as e:
        print(f"Error exporting data for {ticker}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def generate_master_stocks_json(ranked_stocks, shocking_predictions):
    """Generate master JSON file with all analyzed stocks"""
    try:
        os.makedirs('stock_data', exist_ok=True)
        
        stocks_list = []
        for _, row in ranked_stocks.iterrows():
            stocks_list.append({
                "ticker": row['ticker'],
                "name": row['name'],
                "investment_score": round(float(row['investment_score']), 2),
                "sentiment_category": row['sentiment_category'],
                "sentiment_score": round(float(row['avg_sentiment']), 4),
                "rank": int(row['rank']),
                "sector": row.get('sector', 'Unknown'),
                "data_file": f"{row['ticker']}_data.json"
            })
        
        master_data = {
            "stocks": stocks_list,
            "shocking_predictions": shocking_predictions,
            "total_stocks": len(stocks_list),
            "last_updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "analysis_version": "2.0"
        }
        
        with open('stock_data/master_stocks.json', 'w') as f:
            json.dump(master_data, f, indent=2)
        
        print("✓ Generated master stocks JSON")
        return True
    
    except Exception as e:
        print(f"Error generating master JSON: {e}")
        return False


def rank_stocks_by_investment_potential(results_list):
    """Rank stocks by prediction change percentage (highest to lowest)"""
    # Filter out None results
    valid_results = [r for r in results_list if r is not None]
    
    if not valid_results:
        print("No valid results to rank")
        return pd.DataFrame()
    
    # Create DataFrame
    df = pd.DataFrame(valid_results)
    
    # Sort by prediction change percentage (descending - highest to lowest)
    ranked_df = df.sort_values('price_change_pct', ascending=False).reset_index(drop=True)
    ranked_df['rank'] = ranked_df.index + 1
    
    return ranked_df


def _generate_ranking_report(ranked_df):
    """Generate markdown report for rankings"""
    report = "# Stock Investment Ranking Report\n\n"
    report += f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    report += f"**Total Stocks Analyzed:** {len(ranked_df)}\n\n"
    
    # Top 10 recommendations
    report += "## Top 10 Predicted Price Changes\n\n"
    top_10 = ranked_df.head(10)
    
    for _, row in top_10.iterrows():
        report += f"{int(row['rank'])}. **{row['ticker']}** - {row['name']}\n"
        report += f"   - Predicted Change: {row['price_change_pct']:.2f}%\n"
        report += f"   - Investment Score: {row['investment_score']:.2f}/100\n"
        report += f"   - Sentiment: {row['sentiment_category']} ({row['avg_sentiment']:.4f})\n"
        report += f"   - News Articles: {row['news_count']}\n\n"
    
    # Full ranking table
    report += "## Complete Rankings (by Prediction Change %)\n\n"
    report += "| Rank | Ticker | Company | Prediction % | Investment Score | Sentiment | News |\n"
    report += "|------|--------|---------|--------------|-----------------|-----------|------|\n"
    
    for _, row in ranked_df.iterrows():
        report += f"| {int(row['rank'])} | {row['ticker']} | {row['name'][:30]} | "
        report += f"{row['price_change_pct']:.2f}% | {row['investment_score']:.2f} | "
        report += f"{row['sentiment_category']} | {row['news_count']} |\n"
    
    # Methodology
    report += "\n## Methodology\n\n"
    report += "Stocks are ranked by predicted price change percentage (highest to lowest):\n"
    report += "- Prediction change = (Predicted price - Current price) / Current price * 100%\n"
    report += "- Based on machine learning model trained on historical price and sentiment data\n"
    report += "- Investment scores combine sentiment analysis and news volume\n"
    report += "- Historical price momentum and technical indicators also influence predictions\n\n"
    report += "**Disclaimer:** This analysis is for informational purposes only. "
    report += "Always conduct thorough research and consult with financial advisors.\n"
    
    # Save report
    with open('reports/investment_ranking_report.md', 'w') as f:
        f.write(report)
    
    print("✓ Generated ranking report")


def analyze_top_stocks(max_stocks=None):
    """Main analysis pipeline for top stocks"""
    if max_stocks is None:
        max_stocks = MAX_STOCKS
    
    print(f"\n{'='*60}")
    print(f"Starting Stock Analysis Pipeline")
    print(f"{'='*60}\n")
    
    # Step 1: Get top stocks
    print("Step 1: Fetching top stocks...")
    top_stocks = get_top_101_stocks()
    
    if len(top_stocks) > max_stocks:
        top_stocks = top_stocks.head(max_stocks)
    
    print(f"✓ Retrieved {len(top_stocks)} stocks\n")
    
    # Step 2: Analyze sentiment
    print("Step 2: Analyzing sentiment...")
    analyzer = SentimentAnalyzer()
    sentiment_results = []
    all_predictions_data = []
    
    for idx, row in top_stocks.iterrows():
        try:
            ticker_data = row.to_dict()
            ticker = ticker_data['ticker']
            
            print(f"  [{idx+1}/{len(top_stocks)}] Processing {ticker}...")
            
            # Sentiment analysis
            sentiment_result = analyzer.analyze_ticker_sentiment(ticker_data)
            
            # Get price data - explicitly request 90 days (3 months)
            price_data = get_stock_price_data(ticker, days=90)
            
            if price_data is not None and not price_data.empty:
                # Generate predictions - 30 days (1 month) into the future
                prediction_result = predict_stock_trend(
                    ticker,
                    price_data,
                    sentiment_result['avg_sentiment']
                )
                
                if prediction_result:
                    # Store historical and prediction data in sentiment_result
                    # Convert historical prices to list of dicts
                    historical_data = [
                        {
                            'date': date.strftime('%Y-%m-%d'),
                            'price': float(price)
                        }
                        for date, price in price_data['Close'].items()
                    ]
                    
                    # Format prediction data with dates
                    prediction_dates = pd.date_range(
                        start=price_data.index[-1] + pd.Timedelta(days=1),
                        periods=len(prediction_result['predictions'])
                    )
                    
                    prediction_data = {
                        'data': [
                            {
                                'date': date.strftime('%Y-%m-%d'),
                                'price': float(price)
                            }
                            for date, price in zip(prediction_dates, prediction_result['predictions'])
                        ],
                        'upper_bound': [
                            {
                                'date': date.strftime('%Y-%m-%d'),
                                'price': float(price)
                            }
                            for date, price in zip(prediction_dates, prediction_result['upper_bound'])
                        ],
                        'lower_bound': [
                            {
                                'date': date.strftime('%Y-%m-%d'),
                                'price': float(price)
                            }
                            for date, price in zip(prediction_dates, prediction_result['lower_bound'])
                        ]
                    }
                    
                    # Add to sentiment result
                    sentiment_result['historical_data'] = historical_data
                    sentiment_result['prediction'] = prediction_data
                    
                    # Collect for shocking predictions
                    all_predictions_data.append({
                        'ticker': ticker,
                        'name': sentiment_result['name'],
                        'price_change_pct': prediction_result['price_change_pct'],
                        'prediction_direction': prediction_result['prediction_direction'],
                        'current_price': prediction_result['current_price'],
                        'predicted_price_30d': prediction_result['predicted_price_30d'],
                        'sentiment_score': sentiment_result['avg_sentiment'],
                        'investment_score': sentiment_result['investment_score']
                    })
                else:
                    # No predictions available
                    sentiment_result['historical_data'] = []
                    sentiment_result['prediction'] = {'data': [], 'upper_bound': [], 'lower_bound': []}
            else:
                # No price data available
                sentiment_result['historical_data'] = []
                sentiment_result['prediction'] = {'data': [], 'upper_bound': [], 'lower_bound': []}
            
            sentiment_results.append(sentiment_result)
        
        except Exception as e:
            print(f"  ✗ Error processing {ticker}: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n✓ Completed sentiment analysis\n")
    
    # Step 3: Rank stocks
    print("Step 3: Ranking stocks...")
    ranked_stocks = rank_stocks_by_investment_potential(sentiment_results)
    print(f"✓ Ranked {len(ranked_stocks)} stocks\n")
    
    # Step 4: Generate shocking predictions
    print("Step 4: Generating shocking predictions...")
    shocking_predictions = generate_shocking_predictions(all_predictions_data, top_n=5)
    print(f"✓ Identified {len(shocking_predictions['all_shocking'])} shocking predictions\n")
    
    print(f"\n{'='*60}")
    print(f"Analysis Complete!")
    print(f"{'='*60}\n")
    print(f"✓ Data ready for database update\n")
    
    return ranked_stocks, shocking_predictions


if __name__ == "__main__":
    # Run the full analysis
    ranked_stocks, shocking_predictions = analyze_top_stocks(max_stocks=20)
    
    print("\nTop 5 Investments:")
    print(ranked_stocks[['rank', 'ticker', 'name', 'investment_score']].head())
    
    print("\nTop Shocking Increases:")
    for pred in shocking_predictions['top_increases'][:3]:
        print(f"  {pred['symbol']}: +{pred['prediction']:.2f}% over {pred['timeframe']}")
