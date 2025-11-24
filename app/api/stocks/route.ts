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
      // Get specific stock data
      const { data, error } = await supabase.rpc("get_stock_data", {
        p_ticker: ticker,
      });

      if (error) throw error;
      return NextResponse.json(data);
    }

    // Get all stocks sorted by rank
    const { data, error } = await supabase
      .from("stocks")
      .select("ticker, name, sentiment, news_count, rank, investment_score")
      .order("rank", { ascending: true })
      .limit(100);

    if (error) throw error;
    return NextResponse.json(data);
  } catch (error) {
    console.error("Error:", error);
    return NextResponse.json(
      { error: "Failed to fetch stock data" },
      { status: 500 }
    );
  }
}
