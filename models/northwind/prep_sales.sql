WITH orders AS (
				SELECT * FROM staging_orders
),
order_details AS (
				SELECT * FROM staging_order_details
),
products AS (
				SELECT * FROM staging_products
),
categories AS (
				SELECT * FROM staging_categories
),
joined AS (
SELECT o.order_id,
	   o.customer_id,
	   p.product_name,
	   c.category_name,
	   p.unit_price,
	   od.quantity,
	   od.discount,
	   od.unit_price * od.quantity * (1 - od.discount) AS revenue,
	   EXTRACT(YEAR FROM o.order_date)::INT AS order_year,
	   EXTRACT(MONTH FROM o.order_date)::INT AS order_month
FROM staging_orders o 
JOIN staging_order_details od USING (order_id)
JOIN staging_products p USING (product_id) 
LEFT JOIN staging_categories c USING (category_id)
)
SELECT * FROM joined
