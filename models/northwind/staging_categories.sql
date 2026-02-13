WITH columns_formatted AS (
					SELECT categoryid AS category_id,
						   categoryname AS category_name
						   --description,
						   --picture
					FROM {{source('northwind_data', 'categories')}}
)
SELECT * FROM columns_formatted

