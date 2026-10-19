-- Gate 9: the tracking number. Replace the ID with the request you are chasing.
SELECT *
FROM ai_usage
WHERE day >= CAST(current_date - INTERVAL '1' DAY AS varchar)
  AND request_id = 'PASTE-REQUEST-ID';
