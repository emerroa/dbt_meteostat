WITH columns_formatted AS (
						SELECT orderid AS order_id,
							   productid AS product_id,
							   unitprice::NUMERIC AS unit_price,
							   quantity::INT,
							   discount::NUMERIC
						FROM {{source('northwind_data', 'order_details')}}
)
SELECT * 
FROM columns_formatted