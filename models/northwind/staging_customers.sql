WITH customers_raw AS (
					SELECT customer_id,
						   company_name,
						   contact_name,
						   contact_title,
						   address,
						   city, 
						   region,
						   postal_code,
						   country,
						   phone,
						   fax
					FROM {{source('northwind_data', 'customers')}}
),
customers_formatted AS (
					SELECT customer_id,
						   company_name,
						   contact_name,
						   contact_title,
						   address,
						   city, 
						   region,
						   postal_code,
						   country,
						   regexp_replace(phone, '\D', '', 'g') AS formatted_phone, 
						   -- Anything that's not a digit \D, '' replaces the non-digits with nothing, 'g' does it for every hypen/space found
						   regexp_replace(fax, '\D', '', 'g') AS formatted_fax
					FROM customers_raw
)
SELECT * 
FROM customers_formatted
