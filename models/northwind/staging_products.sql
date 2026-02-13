WITH products_formatted AS (
					SELECT productid::VARCHAR AS product_id,
						   productname AS product_name,
						   supplierid::VARCHAR AS supplier_id,
						   categoryid AS category_id,
						   substring(quantityperunit from '^(\d+)')::INT AS product_quantity,
					    	-- Extract everything AFTER the first number as the unit
						   trim(regexp_replace(
						   regexp_replace(quantityperunit, '^\d+', ''), -- Remove numbers
						   '^[^a-zA-Z0-9]+', ''                         -- Remove everything at the start that isn't a number or letter
						   )) AS unit_description,
						   unitprice::MONEY AS unit_price,
						   unitsinstock::INT AS units_in_stock,
						   unitsonorder::INT AS units_on_order,
						   reorderlevel AS reorder_level
					FROM {{source('northwind_data', 'products')}}
)
SELECT * FROM products_formatted