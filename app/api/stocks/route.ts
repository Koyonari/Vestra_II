import { createClient } from "@supabase/supabase-js";
import { NextResponse } from "next/server";

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
);

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const ticker = searchParams.get("ticker");

  try {
    if (ticker) {
      // Get specific stock data with historical prices and predictions
      const { data: stockData, error: stockError } = await supabase
        .from("stocks")
        .select(
          "ticker, name, sentiment, news_count, rank, investment_score, last_updated"
        )
        .eq("ticker", ticker)
        .single();

      if (stockError) throw stockError;
      if (!stockData) {
        return NextResponse.json(
          { error: `Stock ${ticker} not found` },
          { status: 404 }
        );
      }

      // Get historical prices
      const { data: historicalData, error: histError } = await supabase
        .from("stock_prices")
        .select("date, price")
        .eq("ticker", ticker)
        .order("date", { ascending: true });

      if (histError) throw histError;

      // Get predictions
      const { data: predictions, error: predError } = await supabase
        .from("stock_predictions")
        .select("date, price, upper_bound, lower_bound")
        .eq("ticker", ticker)
        .order("date", { ascending: true });

      if (predError) throw predError;

      // Validate that we have historical data
      if (!historicalData || historicalData.length === 0) {
        return NextResponse.json(
          { error: `No historical data available for ${ticker}` },
          { status: 404 }
        );
      }

      // Format response with sentiment nested and prediction data organized
      interface HistoricalPrice {
        date: string;
        price: number;
      }

      interface Prediction {
        date: string;
        price: number;
        upper_bound: number;
        lower_bound: number;
      }

      const formattedHistoricalData =
        historicalData?.map((item: HistoricalPrice) => ({
          date: item.date,
          price: item.price,
        })) || [];

      const formattedPredictions =
        predictions?.map((item: Prediction) => ({
          date: item.date,
          price: item.price,
        })) || [];

      const formattedUpperBound =
        predictions?.map((item: Prediction) => ({
          date: item.date,
          price: item.upper_bound,
        })) || [];

      const formattedLowerBound =
        predictions?.map((item: Prediction) => ({
          date: item.date,
          price: item.lower_bound,
        })) || [];

      return NextResponse.json({
        ticker: stockData.ticker,
        name: stockData.name,
        sentiment: stockData.sentiment,
        news_count: stockData.news_count,
        rank: stockData.rank,
        investment_score: stockData.investment_score,
        last_updated: stockData.last_updated,
        historical_data: formattedHistoricalData,
        prediction: {
          data: formattedPredictions,
          upper_bound: formattedUpperBound,
          lower_bound: formattedLowerBound,
        },
      });
    }

    // Get all stocks sorted by rank (all 101)
    const { data: stocks, error } = await supabase
      .from("stocks")
      .select("ticker, name, sentiment, news_count, rank, investment_score")
      .order("rank", { ascending: true });

    if (error) throw error;

    // Calculate prediction percentage change for each stock
    const stocksWithPredictions = await Promise.all(
      stocks.map(async (stock) => {
        try {
          // Get the latest prediction
          const { data: predictions } = await supabase
            .from("stock_predictions")
            .select("price")
            .eq("ticker", stock.ticker)
            .order("date", { ascending: false })
            .limit(1);

          // Get the current price
          const { data: historicalData } = await supabase
            .from("stock_prices")
            .select("price")
            .eq("ticker", stock.ticker)
            .order("date", { ascending: false })
            .limit(1);

          if (!predictions?.length || !historicalData?.length) {
            return { ...stock, prediction_change: 0 };
          }

          const currentPrice = historicalData[0].price;
          const predictedPrice = predictions[0].price;
          const percentChange =
            ((predictedPrice - currentPrice) / currentPrice) * 100;

          return { ...stock, prediction_change: percentChange };
        } catch (error) {
          console.error(
            `Error calculating prediction for ${stock.ticker}:`,
            error
          );
          return { ...stock, prediction_change: 0 };
        }
      })
    );

    return NextResponse.json(stocksWithPredictions);
  } catch (error) {
    console.error("Error:", error);
    return NextResponse.json(
      { error: "Failed to fetch stock data" },
      { status: 500 }
    );
  }
}
