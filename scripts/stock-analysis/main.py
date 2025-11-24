#!/usr/bin/env python3
"""
Main entry point for stock analysis pipeline
Runs complete analysis and writes to database
"""

import sys
import os
from datetime import datetime
import traceback

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from generate_data import analyze_top_stocks
from database import DatabaseManager
from config import MAX_STOCKS


def main():
    """Run the complete stock analysis pipeline"""
    try:
        print("\n" + "="*70)
        print(f"STOCK ANALYSIS PIPELINE")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70 + "\n")
        
        # Step 1: Run analysis
        print("Phase 1: Analyzing stocks...")
        ranked_stocks, shocking_predictions = analyze_top_stocks(max_stocks=MAX_STOCKS)
        
        if ranked_stocks.empty:
            print("✗ No stocks were successfully analyzed. Exiting.")
            sys.exit(1)
        
        print(f"\n✓ Successfully analyzed {len(ranked_stocks)} stocks")
        
        # Step 2: Write to database
        print("\nPhase 2: Updating database...")
        db = DatabaseManager()
        success_count, error_count = db.write_analysis_to_database(ranked_stocks, shocking_predictions)
        
        # Step 3: Summary
        print("\n" + "="*70)
        print("PIPELINE SUMMARY")
        print("="*70)
        print(f"Stocks analyzed: {len(ranked_stocks)}")
        print(f"Database writes successful: {success_count}")
        print(f"Database writes failed: {error_count}")
        
        if not ranked_stocks.empty:
            print(f"\nTop investment: {ranked_stocks.iloc[0]['ticker']} (Score: {ranked_stocks.iloc[0]['investment_score']:.2f})")
        
        if shocking_predictions['top_increases']:
            top_increase = shocking_predictions['top_increases'][0]
            print(f"Biggest predicted increase: {top_increase['symbol']} (+{top_increase['prediction']:.2f}%)")
        
        if shocking_predictions['top_decreases']:
            top_decrease = shocking_predictions['top_decreases'][0]
            print(f"Biggest predicted decrease: {top_decrease['symbol']} (-{top_decrease['prediction']:.2f}%)")
        
        print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70 + "\n")
        
        # Exit with appropriate code
        sys.exit(0 if error_count == 0 else 1)
        print(f"STOCK ANALYSIS PIPELINE")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70 + "\n")
        
        # Step 1: Run analysis
        print("Phase 1: Analyzing stocks...")
        ranked_stocks, shocking_predictions = analyze_top_stocks(max_stocks=MAX_STOCKS)
        
        if ranked_stocks.empty:
            print("✗ No stocks were successfully analyzed. Exiting.")
            sys.exit(1)
        
        print(f"\n✓ Successfully analyzed {len(ranked_stocks)} stocks")
        
        # Step 2: Write to database
        print("\nPhase 2: Writing to database...")
        db = DatabaseManager()
        success_count, error_count = db.write_analysis_to_database(
            ranked_stocks,
            shocking_predictions
        )
        
        # Step 3: Summary
        print("\n" + "="*70)
        print("PIPELINE SUMMARY")
        print("="*70)
        print(f"Stocks analyzed: {len(ranked_stocks)}")
        print(f"Database writes successful: {success_count}")
        print(f"Database writes failed: {error_count}")
        print(f"Top investment: {ranked_stocks.iloc[0]['ticker']} (Score: {ranked_stocks.iloc[0]['investment_score']:.2f})")
        
        if shocking_predictions['top_increases']:
            top_increase = shocking_predictions['top_increases'][0]
            print(f"Biggest predicted increase: {top_increase['symbol']} (+{top_increase['prediction']:.2f}%)")
        
        if shocking_predictions['top_decreases']:
            top_decrease = shocking_predictions['top_decreases'][0]
            print(f"Biggest predicted decrease: {top_decrease['symbol']} (-{top_decrease['prediction']:.2f}%)")
        
        print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70 + "\n")
        
        # Exit with appropriate code
        sys.exit(0 if error_count == 0 else 1)
    
    except KeyboardInterrupt:
        print("\n\n⚠ Pipeline interrupted by user")
        sys.exit(130)
    
    except Exception as e:
        print("\n" + "="*70)
        print("PIPELINE FAILED")
        print("="*70)
        print(f"Error: {str(e)}")
        print("\nFull traceback:")
        traceback.print_exc()
        print("="*70 + "\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
