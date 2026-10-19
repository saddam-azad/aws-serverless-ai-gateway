-- Who used the tokens today?
SELECT app, SUM(input_tokens + output_tokens) AS tokens, COUNT(*) AS requests
FROM ai_usage
WHERE day = CAST(current_date AS varchar)
GROUP BY app
ORDER BY tokens DESC;
