WITH durations as (
			SELECT origin,
				   dest,
				   MAKE_INTERVAL(mins => actual_elapsed_time::INT) AS f_actual_elapsed_time,
				   MAKE_INTERVAL(mins => arr_delay::INT) AS f_arr_delay
			FROM {{ref('prep_flights')}}
			GROUP BY origin, dest, f_actual_elapsed_time, f_arr_delay
),
aggregations AS (
			SELECT origin,
				   dest,
				   AVG(f_actual_elapsed_time) AS avg_f_elapsed_time,
				   AVG(f_arr_delay) AS avg_arr_delay,
				   MAX(f_arr_delay) AS max_delay,
				   MIN(f_arr_delay) AS min_delay
			FROM durations 
			GROUP BY origin, dest
)
SELECT f.origin AS origin_airport,
	   ai1.city AS origin_city,
	   ai1.country AS origin_country,
	   ai1.name AS origin_airport_name,
	   f.dest AS dest_airport,
	   ai2.city AS dest_city,
	   ai2.country AS dest_country,
	   ai2.name AS dest_airport_name,
	   COUNT(*) AS total_flights_p_route,
	   COUNT(DISTINCT tail_number) AS unique_airplanes,
	   COUNT(DISTINCT airline) AS unique_airlines,
	   avg_f_elapsed_time,
	   avg_arr_delay,
	   max_delay,
	   min_delay,
	   SUM(cancelled) AS total_n_cancelled,
	   SUM(diverted) AS total_n_diverted
FROM aggregations AS ag, {{ref('prep_flights')}} AS f
JOIN {{ref('prep_airports')}} AS ai1
ON f.origin = ai1.faa
JOIN {{ref('prep_airports')}} AS ai2
ON f.dest = ai2.faa
GROUP BY f.origin, f.dest, 
		 ai1.city, ai1.country, ai1.name, 
		 ai2.city, ai2.country, ai2.name, 
		 avg_f_elapsed_time, 
		 avg_arr_delay,
		 max_delay, 
		 min_delay
