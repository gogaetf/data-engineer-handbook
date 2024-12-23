-- select average number of hits per host
WITH avg_per_user AS
(
	SELECT
		ip, 
	    host,
	    AVG(num_hits) AS avg_num_hits
	FROM processed_events_session_aggregated
	GROUP BY 
		ip, 
	    host
	ORDER BY avg_num_hits
)
SELECT
    host,
    AVG(avg_num_hits) AS avg_num_hits
FROM avg_per_user
GROUP BY 
    host
ORDER BY avg_num_hits;

