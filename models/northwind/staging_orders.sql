WITH columns_formatted AS (
						SELECT orderid AS order_id,
							   customerid AS customer_id,
							   employeeid AS employee_id,
							   orderdate::DATE AS order_date,
							   requireddate::DATE AS required_date,
							   shippeddate::DATE AS shipped_date,
							   shipvia AS ship_via,
							   --replace(freight::TEXT, '.', '') AS freight_f, -- i'm leaving as a text because i'm asumming it's a code
							   --shipname AS ship_name,
							   --shipaddress AS ship_address,
							   shipcity AS ship_city,
							   --shipregion AS ship_region,
							   --shippostalcode AS ship_postal_code,
							   shipcountry AS ship_country
						FROM {{source('northwind_data', 'orders')}}
)
SELECT * FROM columns_formatted