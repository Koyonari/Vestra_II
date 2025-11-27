"use client";

import { useState, useMemo, useEffect, useCallback } from "react";
import {
  TrendingUp,
  TrendingDown,
  ChevronLeft,
  ChevronRight,
} from "lucide-react";
import { Button } from "@/components/ui/button";

interface Prediction {
  id?: string;
  company: string;
  symbol: string;
  prediction: number;
  direction: "increase" | "decrease";
  timeframe: string;
  timestamp: string;
  logoClass?: string;
}

interface ApiPrediction {
  symbol: string;
  company: string;
  prediction: number;
  direction: string;
  timeframe: string;
  timestamp: string;
  investment_score: number;
  position: number;
}

export default function PredictionsList() {
  const [predictions, setPredictions] = useState<Prediction[]>([]);
  const [isLoading, setIsLoading] = useState(true);

  // Pagination logic
  const itemsPerPage = 3;
  const [currentPage, setCurrentPage] = useState(1);

  const generateLogoClass = useCallback(
    (symbol: string, investmentScore: number): string => {
      if (investmentScore > 70) return "bg-green-100";
      if (investmentScore > 50) return "bg-yellow-100";
      if (investmentScore > 30) return "bg-orange-100";
      return "bg-red-100";
    },
    []
  );

  const fetchPredictions = useCallback(async () => {
    try {
      const response = await fetch("/api/predictions?limit=10");
      if (!response.ok) throw new Error("Failed to fetch predictions");

      const data = await response.json();

      // Combine top increases and decreases
      const allPredictions: Prediction[] = [
        ...(data.top_increases || []),
        ...(data.top_decreases || []),
      ].map((pred: ApiPrediction, idx: number) => ({
        id: `${pred.symbol}-${idx}`,
        company: pred.company,
        symbol: pred.symbol,
        prediction: pred.prediction,
        direction: pred.direction as "increase" | "decrease",
        timeframe: pred.timeframe,
        timestamp: pred.timestamp
          ? new Date(pred.timestamp).toLocaleString("en-US", {
              year: "numeric",
              month: "short",
              day: "numeric",
              hour: "2-digit",
              minute: "2-digit",
            })
          : "Recently",
        logoClass: generateLogoClass(pred.symbol, pred.investment_score),
      }));

      setPredictions(allPredictions);
    } catch (error) {
      console.error("Error loading predictions:", error);
      // Fallback to empty state
      setPredictions([]);
    } finally {
      setIsLoading(false);
    }
  }, [generateLogoClass]);

  useEffect(() => {
    fetchPredictions();
  }, [fetchPredictions]);

  const totalPages = useMemo(
    () => Math.ceil(predictions.length / itemsPerPage),
    [predictions.length]
  );

  const paginatedPredictions = useMemo(() => {
    const start = (currentPage - 1) * itemsPerPage;
    const end = start + itemsPerPage;
    return predictions.slice(start, end);
  }, [currentPage, predictions]);

  const nextPage = () => {
    if (currentPage < totalPages) {
      setCurrentPage(currentPage + 1);
    }
  };

  const prevPage = () => {
    if (currentPage > 1) {
      setCurrentPage(currentPage - 1);
    }
  };

  if (isLoading) {
    return (
      <div className="h-[360px] flex items-center justify-center">
        <div className="animate-spin rounded-full h-8 w-8 border-t-2 border-b-2 border-primary"></div>
      </div>
    );
  }

  if (predictions.length === 0) {
    return (
      <div className="h-[360px] flex items-center justify-center text-gray-500">
        <p>No shocking predictions available</p>
      </div>
    );
  }

  return (
    <div className="h-[360px] flex flex-col justify-between">
      <div className="space-y-4 overflow-y-auto">
        {paginatedPredictions.map((prediction) => (
          <div
            key={prediction.id}
            className="flex items-center gap-4 p-3 border-b border-copper/20 last:border-none"
          >
            <div
              className={`${
                prediction.logoClass || "bg-gray-200"
              } h-10 w-10 rounded-full flex items-center justify-center`}
            >
              <span className="text-sm font-semibold">
                {prediction.symbol.substring(0, 1)}
              </span>
            </div>
            <div className="flex-1">
              <div className="flex items-center gap-2">
                {prediction.direction === "increase" ? (
                  <TrendingUp className="h-4 w-4 text-green-500" />
                ) : (
                  <TrendingDown className="h-4 w-4 text-red-500" />
                )}
                <span className="font-medium">
                  {prediction.symbol} is predicted to{" "}
                  <span
                    className={
                      prediction.direction === "increase"
                        ? "text-green-500"
                        : "text-red-500"
                    }
                  >
                    {prediction.direction}
                  </span>{" "}
                  by {prediction.prediction.toFixed(2)}% in the next{" "}
                  {prediction.timeframe}
                </span>
              </div>
              <div className="text-xs text-gray-500 mt-1">
                {prediction.timestamp}
              </div>
            </div>
          </div>
        ))}
      </div>
      {/* Pagination Controls */}
      <div className="flex justify-between items-center mt-4 pt-2 border-t">
        <Button
          variant="outline"
          size="sm"
          disabled={currentPage === 1}
          onClick={prevPage}
        >
          <ChevronLeft className="h-4 w-4" />
        </Button>
        <span className="text-sm">
          Page {currentPage} of {totalPages}
        </span>
        <Button
          variant="outline"
          size="sm"
          disabled={currentPage === totalPages}
          onClick={nextPage}
        >
          <ChevronRight className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
