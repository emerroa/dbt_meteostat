WITH o_details_formatted AS (
						SELECT orderid::VARCHAR AS order_id,
							   productid::VARCHAR AS product_id,
							   unitprice::MONEY AS unit_price,
							   quantity::INT,
							   discount::FLOAT
						FROM {{source('northwind_data', 'order_details')}}
)
SELECT * 
FROM o_details_formatted