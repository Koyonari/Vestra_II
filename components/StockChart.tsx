"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import {
  createChart,
  ColorType,
  LineStyle,
  PriceScaleMode,
  type IChartApi,
  LineSeries,
  AreaSeries,
  type ISeriesApi,
} from "lightweight-charts";

interface ChartDataPoint {
  date: string;
  price: number;
}

interface ChartLoadedData {
  name: string;
  ticker: string;
  historical_data: ChartDataPoint[];
  currentPrice: number;
  sentiment: {
    score: number;
    category: string;
    investment_score: number;
  };
  last_updated: string;
}

interface StockChartProps {
  stockSymbol: string;
  timeframe: string;
  onChartLoaded?: (data: ChartLoadedData) => void;
}

export default function StockChart({
  stockSymbol,
  onChartLoaded,
}: StockChartProps) {
  const [errorMessage, setErrorMessage] = useState("");

  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chart = useRef<IChartApi | null>(null);
  const resizeObserver = useRef<ResizeObserver | null>(null);
  const currentSymbol = useRef<string>("");
  const onChartLoadedRef = useRef(onChartLoaded);
  const seriesRef = useRef<ISeriesApi<"Line" | "Area">[]>([]);

  // Update the ref when callback changes
  useEffect(() => {
    onChartLoadedRef.current = onChartLoaded;
  }, [onChartLoaded]);

  const handleResize = useCallback(() => {
    if (chart.current && chartContainerRef.current) {
      chart.current.applyOptions({
        width: chartContainerRef.current.clientWidth,
      });
    }
  }, []);

  // Fetch data and create/update chart
  useEffect(() => {
    const fetchChartData = async () => {
      // Only clear chart if symbol changed
      if (currentSymbol.current !== stockSymbol) {
        if (chart.current) {
          chart.current.remove();
          chart.current = null;
        }
        seriesRef.current = [];
        currentSymbol.current = stockSymbol;
      }

      setErrorMessage("");

      try {
        const response = await fetch(`/api/stocks?ticker=${stockSymbol}`);

        if (response.ok) {
          const data = await response.json();

          // Validate that we have historical data
          if (!data.historical_data || data.historical_data.length === 0) {
            throw new Error(`No historical data available for ${stockSymbol}`);
          }

          const historical = data.historical_data.map(
            (item: { date: string; price: number }) => ({
              date: item.date,
              price: Number(item.price),
            })
          );

          const prediction =
            data.prediction?.data?.map(
              (item: { date: string; price: number }) => ({
                date: item.date,
                price: Number(item.price),
              })
            ) || [];

          const upperBound =
            data.prediction?.upper_bound?.map(
              (item: { date: string; price: number }) => ({
                date: item.date,
                price: Number(item.price),
              })
            ) || [];

          const lowerBound =
            data.prediction?.lower_bound?.map(
              (item: { date: string; price: number }) => ({
                date: item.date,
                price: Number(item.price),
              })
            ) || [];

          // Create chart if it doesn't exist
          if (!chart.current && chartContainerRef.current) {
            const container = chartContainerRef.current;

            const chartOptions = {
              width: container.clientWidth || 800,
              height: container.clientHeight || 300,
              layout: {
                background: { type: ColorType.Solid, color: "transparent" },
                textColor: "#d1d5db",
              },
              grid: {
                vertLines: {
                  color: "rgba(42, 46, 57, 0.2)",
                  style: LineStyle.Dotted,
                },
                horzLines: {
                  color: "rgba(42, 46, 57, 0.2)",
                  style: LineStyle.Dotted,
                },
              },
              rightPriceScale: {
                borderColor: "rgba(197, 203, 206, 0.8)",
                mode: PriceScaleMode.Normal,
              },
              timeScale: {
                borderColor: "rgba(197, 203, 206, 0.8)",
                timeVisible: true,
                fixLeftEdge: true,
                fixRightEdge: true,
                rightBarStaysOnScroll: true,
              },
              crosshair: {
                mode: 0,
              },
            };

            chart.current = createChart(container, chartOptions);
          }

          if (chart.current) {
            // Clear existing series
            seriesRef.current.forEach((series) => {
              try {
                chart.current?.removeSeries(series);
              } catch {
                // Series may already be removed
              }
            });
            seriesRef.current = [];

            // Add historical data series
            const mainSeries = chart.current.addSeries(AreaSeries, {
              lineColor: "#2962FF",
              topColor: "#2962FF",
              bottomColor: "rgba(41, 98, 255, 0.28)",
              lineWidth: 2,
            });
            seriesRef.current.push(mainSeries);

            const formattedHistoricalData = historical.map(
              (item: ChartDataPoint) => ({
                time: item.date,
                value: item.price,
              })
            );

            mainSeries.setData(formattedHistoricalData);

            // Add prediction data if available
            if (prediction.length > 0) {
              const predictionSeries = chart.current.addSeries(LineSeries, {
                color: "#FF9800",
                lineWidth: 2,
                lineStyle: LineStyle.Dashed,
              });
              seriesRef.current.push(predictionSeries);

              const formattedPredictionData = prediction.map(
                (item: ChartDataPoint) => ({
                  time: item.date,
                  value: item.price,
                })
              );

              predictionSeries.setData(formattedPredictionData);

              // Add upper bound
              if (upperBound.length > 0) {
                const upperBoundSeries = chart.current.addSeries(LineSeries, {
                  color: "rgba(76, 175, 80, 0.5)",
                  lineWidth: 1,
                  lineStyle: LineStyle.Dotted,
                });
                seriesRef.current.push(upperBoundSeries);

                const formattedUpperBoundData = upperBound.map(
                  (item: ChartDataPoint) => ({
                    time: item.date,
                    value: item.price,
                  })
                );

                upperBoundSeries.setData(formattedUpperBoundData);
              }

              // Add lower bound
              if (lowerBound.length > 0) {
                const lowerBoundSeries = chart.current.addSeries(LineSeries, {
                  color: "rgba(244, 67, 54, 0.5)",
                  lineWidth: 1,
                  lineStyle: LineStyle.Dotted,
                });
                seriesRef.current.push(lowerBoundSeries);

                const formattedLowerBoundData = lowerBound.map(
                  (item: ChartDataPoint) => ({
                    time: item.date,
                    value: item.price,
                  })
                );

                lowerBoundSeries.setData(formattedLowerBoundData);
              }
            }

            chart.current.timeScale().fitContent();
          }

          if (onChartLoadedRef.current) {
            onChartLoadedRef.current({
              name: data.name,
              ticker: data.ticker,
              historical_data: data.historical_data,
              currentPrice:
                data.historical_data[data.historical_data.length - 1].price,
              sentiment: data.sentiment,
              last_updated: data.last_updated,
            });
          }
        } else {
          throw new Error(`Failed to fetch data for ${stockSymbol}`);
        }
      } catch (error) {
        console.error("Error fetching chart data:", error);
        setErrorMessage(`Failed to fetch data for ${stockSymbol}`);
      }
    };

    fetchChartData();
  }, [stockSymbol]);

  // Setup resize observer
  useEffect(() => {
    const container = chartContainerRef.current;
    if (!container) return;

    resizeObserver.current = new ResizeObserver(handleResize);
    resizeObserver.current.observe(container);
    window.addEventListener("resize", handleResize);

    return () => {
      if (resizeObserver.current && container) {
        resizeObserver.current.unobserve(container);
      }
      window.removeEventListener("resize", handleResize);
      if (chart.current) {
        chart.current.remove();
      }
    };
  }, [handleResize]);

  return (
    <div className="relative w-full h-full">
      {errorMessage && (
        <div className="absolute top-0 left-0 bg-yellow-100 text-yellow-800 p-2 text-xs rounded z-10">
          {errorMessage}
        </div>
      )}
      <div
        ref={chartContainerRef}
        className="w-full h-full"
        style={{ minHeight: "300px", minWidth: "300px" }}
      />
    </div>
  );
}
