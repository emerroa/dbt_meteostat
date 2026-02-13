WITH departures AS (
				SELECT origin,
					   dest, 
					   COUNT (DISTINCT dest) AS u_dep_connections,
					   COUNT(*) AS total_f_planned,
					   SUM(cancelled) AS total_f_cancelled,
					   SUM(diverted) AS total_f_diverted,
					   (COUNT (*) - SUM(cancelled)) - SUM(diverted) AS total_actual_dep
				FROM {{ref('prep_flights')}}
				WHERE origin IN ('JFK', 'LAX', 'MIA')
				GROUP BY origin, dest
),
arrivals AS (
				SELECT dest,
					   origin,  
					   COUNT (DISTINCT origin) AS u_arr_connections,
					   COUNT(*) AS total_f_planned,
					   SUM(cancelled) AS total_cancelled,
					   SUM(diverted) AS total_f_diverted,
					   (COUNT(*) - SUM(cancelled)) - SUM(diverted) AS total_actual_arr
				FROM {{ref('prep_flights')}}
				WHERE origin IN ('JFK', 'LAX', 'MIA')
				GROUP BY dest, origin
),
aggregations AS (
				SELECT AVG(u_dep_connections) AS avg_u_dep_con,
					   AVG(u_arr_connections) AS avg_u_arr_con
				FROM arrivals, departures
)
SELECT ai1.city AS origin_city,
	   ai1.country AS origin_country,
	   ai1.name AS origin_airport_name,
	   SUM(u_dep_connections) AS total_unique_dep_con,
	   avg_u_dep_con,
	   ai2.city AS dest_city,
	   ai2.country AS dest_country,
	   ai2.name AS dest_airport_name,
	   SUM(u_arr_connections) AS total_unique_arr_con, 
	   avg_u_arr_con
FROM {{ref('prep_flights')}} f
CROSS JOIN aggregations
JOIN departures d ON f.origin = d.origin AND f.dest = d.dest
JOIN arrivals a ON f.origin = a.origin AND f.dest = a.dest
JOIN airports ai1 ON f.origin = ai1.faa
JOIN airports ai2 ON f.dest = ai2.faa
GROUP BY origin_city, origin_country, origin_airport_name, 
		 dest_city, dest_country, dest_airport_name,
		 avg_u_dep_con, avg_u_arr_con


