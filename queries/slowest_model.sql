-- Which model is slowest? Percentiles, not averages.
SELECT model,
       COUNT(*) AS requests,
       approx_percentile(ttft_ms, 0.95) AS p95_ttft_ms,
       approx_percentile(latency_ms, 0.50) AS p50_latency_ms,
       approx_percentile(latency_ms, 0.95) AS p95_latency_ms,
       approx_percentile(latency_ms, 0.99) AS p99_latency_ms
FROM ai_usage
WHERE day = CAST(current_date AS varchar) AND status = 200
GROUP BY model
ORDER BY p95_latency_ms DESC;
