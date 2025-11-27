import { createClient } from "@supabase/supabase-js";
import { NextResponse } from "next/server";

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
);

interface StockPredictionData {
  symbol: string;
  company: string;
  prediction: number;
  direction: "increase" | "decrease";
  timeframe: string;
  timestamp: string;
  current_price: number;
  predicted_price: number;
  sentiment_score: number;
  investment_score: number;
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const topN = parseInt(searchParams.get("topN") || "5");

  try {
    // Fetch all stocks with their predictions
    const { data: stocks, error: stocksError } = await supabase
      .from("stocks")
      .select("ticker, name, sentiment, investment_score, rank, news_count")
      .order("rank", { ascending: true })
      .limit(100);

    if (stocksError) throw stocksError;
    if (!stocks || stocks.length === 0) {
      return NextResponse.json({
        top_increases: [],
        top_decreases: [],
        all_shocking: [],
      });
    }

    // Calculate shocking predictions for each stock
    const predictionPromises = stocks.map(async (stock) => {
      try {
        // Get the latest prediction for this stock
        const { data: predictions, error: predError } = await supabase
          .from("stock_predictions")
          .select("date, price")
          .eq("ticker", stock.ticker)
          .order("date", { ascending: false })
          .limit(1); // Get the latest prediction

        if (predError || !predictions || predictions.length === 0) {
          return null;
        }

        // Get the most recent historical price (current price)
        const { data: historicalData, error: histError } = await supabase
          .from("stock_prices")
          .select("price")
          .eq("ticker", stock.ticker)
          .order("date", { ascending: false })
          .limit(1);

        if (histError || !historicalData || historicalData.length === 0) {
          return null;
        }

        const currentPrice = historicalData[0].price;
        const predictedPrice = predictions[0].price;
        const priceChange = predictedPrice - currentPrice;
        const percentChange = (priceChange / currentPrice) * 100;

        // Determine timeframe based on magnitude
        let timeframe = "7 days";
        if (Math.abs(percentChange) > 20) {
          timeframe = "30 days";
        } else if (Math.abs(percentChange) > 10) {
          timeframe = "14 days";
        }

        const direction = percentChange >= 0 ? "increase" : "decrease";
        const sentimentScore =
          typeof stock.sentiment === "object" && stock.sentiment?.score
            ? stock.sentiment.score
            : 0;
        const investmentScore =
          typeof stock.sentiment === "object" &&
          stock.sentiment?.investment_score
            ? stock.sentiment.investment_score
            : 50;

        return {
          symbol: stock.ticker,
          company: stock.name,
          prediction: Math.abs(percentChange),
          direction,
          timeframe,
          timestamp: new Date().toISOString(),
          current_price: currentPrice,
          predicted_price: predictedPrice,
          sentiment_score: sentimentScore,
          investment_score: investmentScore,
        };
      } catch (error) {
        console.error(
          `Error calculating prediction for ${stock.ticker}:`,
          error
        );
        return null;
      }
    });

    const allPredictions = await Promise.all(predictionPromises);
    const validPredictions = allPredictions.filter(
      (p): p is StockPredictionData => p !== null
    );

    // Sort by prediction magnitude
    validPredictions.sort((a, b) => b.prediction - a.prediction);

    // Split into increases and decreases
    const increases = validPredictions
      .filter((p) => p.direction === "increase")
      .slice(0, topN);
    const decreases = validPredictions
      .filter((p) => p.direction === "decrease")
      .slice(0, topN);

    return NextResponse.json({
      top_increases: increases,
      top_decreases: decreases,
      all_shocking: validPredictions.slice(0, topN * 2),
    });
  } catch (error) {
    console.error("Error:", error);
    return NextResponse.json(
      { error: "Failed to fetch predictions" },
      { status: 500 }
    );
  }
}
