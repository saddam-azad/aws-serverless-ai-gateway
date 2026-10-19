-- Clean 429s, 403s, cache hits and fallbacks per tenant.
SELECT tenant,
       COUNT(*) AS requests,
       COUNT_IF(status = 200) AS ok,
       COUNT_IF(status = 429) AS quota_429,
       COUNT_IF(status = 403) AS denied_403,
       COUNT_IF(status >= 500) AS errors,
       COUNT_IF(cache = 'hit') AS cache_hits,
       COUNT_IF(fallback) AS fallbacks
FROM ai_usage
WHERE day = CAST(current_date AS varchar)
GROUP BY tenant
ORDER BY requests DESC;
