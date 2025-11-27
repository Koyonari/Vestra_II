"use client";

import { useState, useEffect, useMemo, useCallback } from "react";
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardContent,
} from "@/components/ui/card";
import { TrendingUp, TrendingDown, LineChart } from "lucide-react";
import StockChart from "@/components/StockChart";
import StockPredictionList from "@/components/StockPredictionList";
import SentimentTable from "@/components/SentimentTable";

interface StockData {
  name: string;
  ticker: string;
  currentPrice: number;
  change: number;
  changePercent: number;
  sentiment: {
    score: number;
    category: string;
    investment_score: number;
  };
  historical_data: Array<{ date: string; price: number }>;
  prediction: {
    data: Array<{ date: string; price: number }>;
    upper_bound: Array<{ date: string; price: number }>;
    lower_bound: Array<{ date: string; price: number }>;
  };
  last_updated: string;
}

export default function Dashboard() {
  // State variables
  const [selectedStock, setSelectedStock] = useState<string>("");
  const [isLoading, setIsLoading] = useState(true);
  const [stockData, setStockData] = useState<StockData | null>(null);
  const [selectedTimeframe] = useState("1M");
  const [currentPrice, setCurrentPrice] = useState(0);
  const [errorMessage, setErrorMessage] = useState<string>("");

  // Fetch list of top stocks to find the 6th ranked one
  const fetchDefaultStock = useCallback(async () => {
    try {
      const response = await fetch("/api/stocks");
      if (!response.ok) throw new Error("Failed to fetch stocks");

      const stocks = await response.json();
      // Find the stock with rank 6
      interface StockListItem {
        rank: number;
        ticker: string;
      }
      const sixthStock = stocks.find(
        (stock: StockListItem) => stock.rank === 6
      );
      if (sixthStock) {
        setSelectedStock(sixthStock.ticker);
      }
    } catch (error) {
      console.error("Error fetching default stock:", error);
      // Fallback to AAPL if something goes wrong
      setSelectedStock("AAPL");
    }
  }, []);

  // Fetch stock data function
  const fetchStockData = useCallback(async (symbol: string) => {
    setIsLoading(true);
    try {
      const response = await fetch(`/api/stocks?ticker=${symbol}`);

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(
          errorData.error || `Failed to fetch data for ${symbol}`
        );
      }

      const data = await response.json();

      // Validate historical data exists
      if (!data.historical_data || data.historical_data.length === 0) {
        throw new Error(
          `No historical data available for ${symbol}. This stock may not have enough trading data.`
        );
      }

      // Calculate the current price (last historical price)
      const historicalData = data.historical_data;
      const lastPrice = historicalData[historicalData.length - 1].price;
      const prevPrice =
        historicalData[historicalData.length - 2]?.price || lastPrice;
      const change = lastPrice - prevPrice;
      const changePercent = (change / prevPrice) * 100;

      setStockData({
        name: data.name,
        ticker: data.ticker,
        currentPrice: lastPrice,
        change: change,
        changePercent: changePercent,
        sentiment: data.sentiment,
        historical_data: data.historical_data,
        prediction: data.prediction,
        last_updated: data.last_updated,
      });

      setCurrentPrice(lastPrice);
      setErrorMessage("");
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Error fetching stock data";
      console.error("Error fetching stock data:", error);
      setErrorMessage(message);
      setStockData(null);
    } finally {
      setIsLoading(false);
    }
  }, []);

  // Initialize with default stock on mount
  useEffect(() => {
    fetchDefaultStock();
  }, [fetchDefaultStock]);

  // Fetch data when selected stock changes
  useEffect(() => {
    if (selectedStock) {
      fetchStockData(selectedStock);
    }
  }, [selectedStock, fetchStockData]);

  // Format price to ensure proper decimal display
  const formatPrice = (price: number) => {
    return price.toFixed(2);
  };

  // Handle chart loaded event
  const handleChartLoaded = (data: ChartLoadedData) => {
    setStockData((prevData) => ({
      ...prevData!,
      ...data,
    }));
    setCurrentPrice(data.currentPrice);
    setIsLoading(false);
  };

  interface ChartLoadedData {
    name: string;
    ticker: string;
    historical_data: Array<{ date: string; price: number }>;
    currentPrice: number;
    sentiment: {
      score: number;
      category: string;
      investment_score: number;
    };
    last_updated: string;
  }

  // Handle stock selection from table
  const handleStockSelect = (ticker: string) => {
    setSelectedStock(ticker);
  };

  const priceChange = useMemo(() => {
    if (!stockData || !stockData.historical_data?.length) {
      return { value: 0, percent: 0 };
    }

    const historicalData = stockData.historical_data;
    const currentPrice = historicalData[historicalData.length - 1].price;
    const previousPrice =
      historicalData[historicalData.length - 2]?.price || currentPrice;
    const change = currentPrice - previousPrice;
    const percentChange = (change / previousPrice) * 100;

    return {
      value: change.toFixed(2),
      percent: percentChange.toFixed(2),
    };
  }, [stockData]);

  // Compute the sentiment score indicator position (for sentiment display in right panel)
  const sentimentPosition = useMemo(() => {
    if (!stockData || !stockData.sentiment) return "50%";
    // Normalize the score to a percentage (0-100)
    // Assuming score ranges from -1 to 1
    const normalizedScore = ((stockData.sentiment.score + 1) / 2) * 100;
    return `${normalizedScore}%`;
  }, [stockData]);

  // Compute the investment score indicator position (for sentiment display in right panel)
  const investmentPosition = useMemo(() => {
    if (!stockData || !stockData.sentiment) return "50%";
    // Assuming investment_score is already 0-100
    return `${stockData.sentiment.investment_score}%`;
  }, [stockData]);

  // Compute sentiment category color (for sentiment display in right panel)
  const categoryColor = useMemo(() => {
    if (!stockData || !stockData.sentiment) return "text-yellow-500";
    const category = stockData.sentiment.category.toLowerCase();
    if (category.includes("bullish")) return "text-green-500";
    if (category.includes("bearish")) return "text-red-500";
    return "text-yellow-500"; // Neutral
  }, [stockData]);

  // Get the last prediction date and price (for prediction display in right panel)
  const lastPrediction = useMemo(() => {
    if (
      !stockData ||
      !stockData.prediction ||
      !stockData.prediction.data ||
      stockData.prediction.data.length === 0
    ) {
      return { date: "", price: 0 };
    }

    const lastIndex = stockData.prediction.data.length - 1;
    return {
      date: new Date(
        stockData.prediction.data[lastIndex].date
      ).toLocaleDateString(),
      price: stockData.prediction.data[lastIndex].price,
    };
  }, [stockData]);

  // Calculate the prediction change percentage (for prediction display in right panel)
  const predictionChange = useMemo(() => {
    if (!stockData || !lastPrediction.price || !stockData.currentPrice) {
      return { value: "0.00", percent: "0.00" };
    }

    const change = lastPrediction.price - stockData.currentPrice;
    const percentChange = (change / stockData.currentPrice) * 100;
    return {
      value: change.toFixed(2),
      percent: percentChange.toFixed(2),
    };
  }, [stockData, lastPrediction]);

  // Calculate the upper and lower bounds (for prediction display in right panel)
  const bounds = useMemo(() => {
    if (
      !stockData ||
      !stockData.prediction ||
      !stockData.prediction.upper_bound ||
      !stockData.prediction.lower_bound ||
      stockData.prediction.upper_bound.length === 0 ||
      stockData.prediction.lower_bound.length === 0
    ) {
      return { upper: "0.00", lower: "0.00" };
    }

    const lastIndex = stockData.prediction.upper_bound.length - 1;
    return {
      upper: stockData.prediction.upper_bound[lastIndex].price.toFixed(2),
      lower: stockData.prediction.lower_bound[lastIndex].price.toFixed(2),
    };
  }, [stockData]);

  // Determine if the prediction is positive or negative
  const isPredictionPositive = useMemo(() => {
    if (!stockData || !lastPrediction.price || !stockData.currentPrice) {
      return false;
    }
    return lastPrediction.price >= stockData.currentPrice;
  }, [stockData, lastPrediction]);

  return (
    <div className="container mx-auto p-4">
      <h1 className="text-3xl font-bold mb-6 text-copper dark:text-white">
        Vestra Dashboard
      </h1>

      {/* Error Message Display */}
      {errorMessage && (
        <div className="mb-4 p-4 bg-red-50 border border-red-200 rounded-lg">
          <p className="text-red-800 text-sm">
            <strong>Unable to load stock:</strong> {errorMessage}
          </p>
          <p className="text-red-700 text-xs mt-2">
            This stock may not have sufficient data. Try selecting a different
            stock from the table below.
          </p>
        </div>
      )}

      {/* Main Content Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Left Section (2/3 width) */}
        <div className="lg:col-span-2 flex flex-col space-y-4">
          {/* Stock Chart Card */}
          <Card className="bg-background shadow-md flex-grow flex-col flex">
            <CardHeader className="pb-2">
              <div className="flex justify-between items-center">
                <div>
                  <CardTitle className="text-xl font-bold">
                    {selectedStock} Stock Chart
                  </CardTitle>
                  {stockData && (
                    <CardDescription className="flex items-center gap-2">
                      <span>{stockData.name}</span>
                      <span className="font-semibold">
                        ${formatPrice(currentPrice)}
                      </span>
                      <span
                        className={`flex items-center text-sm ${
                          Number(priceChange.value) > 0
                            ? "text-green-500"
                            : "text-red-500"
                        }`}
                      >
                        {Number(priceChange.value) > 0 ? (
                          <TrendingUp className="h-4 w-4 mr-1" />
                        ) : (
                          <TrendingDown className="h-4 w-4 mr-1" />
                        )}
                        {Number(priceChange.value) > 0 ? "+" : ""}
                        {priceChange.value} ({priceChange.percent}%)
                      </span>
                    </CardDescription>
                  )}
                </div>
              </div>
            </CardHeader>
            <CardContent className="p-6">
              <div className="h-72 w-full flex items-center justify-center mb-4">
                {!isLoading ? (
                  <StockChart
                    stockSymbol={selectedStock}
                    timeframe={selectedTimeframe}
                    onChartLoaded={handleChartLoaded}
                  />
                ) : (
                  <div className="h-full w-full flex items-center justify-center">
                    <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-copper"></div>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
          {/* Sentiment Analysis Card with flexible height */}
          {stockData && stockData.sentiment && (
            <Card className="bg-background shadow-md flex-grow flex-col flex">
              <CardHeader>
                <CardTitle className="text-lg font-semibold">
                  Sentiment Analysis
                </CardTitle>
              </CardHeader>
              <CardContent className="p-6 flex-grow flex flex-col justify-between">
                {/* Sentiment Score */}
                <div className="space-y-6 flex-grow">
                  <div className="space-y-3">
                    <div className="flex justify-between text-sm">
                      <span>Bearish</span>
                      <span className={categoryColor}>
                        {stockData.sentiment.category}
                      </span>
                      <span>Bullish</span>
                    </div>
                    <div className="relative h-2 bg-gray-200 dark:bg-gray-700 rounded-full">
                      <div className="absolute w-full h-full flex items-center justify-center">
                        <div
                          className="absolute h-4 w-4 rounded-full bg-blue-500 shadow-md z-10"
                          style={{ left: sentimentPosition }}
                        ></div>
                      </div>
                    </div>
                  </div>
                  {/* Investment Score */}
                  <div className="space-y-3">
                    <div className="flex justify-between text-sm">
                      <span>High Risk</span>
                      <span className="font-medium">
                        Investment Score: {stockData.sentiment.investment_score}
                        /100
                      </span>
                      <span>Low Risk</span>
                    </div>
                    <div className="relative h-2 bg-gray-200 dark:bg-gray-700 rounded-full">
                      <div
                        className="absolute h-full bg-gradient-to-r from-red-500 via-yellow-400 to-green-500 rounded-full"
                        style={{ width: "100%" }}
                      ></div>
                      <div className="absolute w-full h-full flex items-center justify-center">
                        <div
                          className="absolute h-4 w-4 rounded-full bg-white border-2 border-blue-500 shadow-md z-10"
                          style={{ left: investmentPosition }}
                        ></div>
                      </div>
                    </div>
                  </div>
                </div>
                {/* Spacer div to push content to top */}
                <div className="mt-auto pt-4"></div>
              </CardContent>
            </Card>
          )}
        </div>

        {/* Right Section (1/3 width) */}
        <div className="flex flex-col space-y-4 h-full">
          {/* Price Prediction Card */}
          {stockData && stockData.prediction && (
            <Card className="bg-background shadow-md flex-grow flex">
              <CardHeader>
                <CardTitle className="text-lg font-semibold">
                  Price Prediction
                </CardTitle>
              </CardHeader>
              <CardContent className="p-4">
                <div className="space-y-4">
                  <div className="flex justify-between items-center">
                    <span className="text-sm text-gray-500">Current Price</span>
                    {stockData && (
                      <span className="font-medium">
                        ${stockData.currentPrice.toFixed(2)}
                      </span>
                    )}
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-sm text-gray-500">
                      Predicted Price <br />({lastPrediction.date})
                    </span>
                    <span className="font-medium">
                      ${lastPrediction.price.toFixed(2)}
                    </span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-sm text-gray-500">
                      Prediction
                      <br />
                      Change
                    </span>
                    <span
                      className={`font-medium ${
                        isPredictionPositive ? "text-green-500" : "text-red-500"
                      }`}
                    >
                      {isPredictionPositive ? "+" : ""}
                      {predictionChange.value} ({predictionChange.percent}%)
                    </span>
                  </div>
                  <div className="pt-2 border-t border-gray-200 dark:border-gray-700">
                    <div className="text-sm text-gray-500 mb-2">
                      Confidence Interval
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm">Lower Bound</span>
                      <span className="font-medium">${bounds.lower}</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm">Upper Bound</span>
                      <span className="font-medium">${bounds.upper}</span>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Stock Predictions Card */}
          <Card className="bg-background shadow-md flex-col flex">
            <CardHeader>
              <CardTitle className="text-xl font-bold flex items-center gap-2">
                <LineChart className="h-5 w-5 text-copper" />
                Stock Predictions
              </CardTitle>
              <CardDescription>
                Latest AI-powered market predictions
              </CardDescription>
            </CardHeader>
            <CardContent className="p-4 flex-grow">
              <StockPredictionList />
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Sentiment Score Table (Bottom, Full Width) */}
      <Card className="mt-4 bg-background shadow-md">
        <CardHeader>
          <div className="flex items-center gap-2">
            <TrendingUp className="h-5 w-5 text-copper" />
            <CardTitle className="text-xl font-bold">
              Top 100 Sentiment Stocks
            </CardTitle>
          </div>
        </CardHeader>
        <CardContent className="p-4">
          <SentimentTable
            onStockSelect={handleStockSelect}
            selectedTicker={selectedStock}
          />
        </CardContent>
      </Card>
    </div>
  );
}
