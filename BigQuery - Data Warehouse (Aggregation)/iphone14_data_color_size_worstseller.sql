SELECT name, modelid, options_color, options_size, COUNT(*) AS total_orders
FROM `data-engineering-poc-383207.shopee_dataset.iphone_data`
GROUP BY name, modelid, options_color, options_size
ORDER BY total_orders ASC
LIMIT 1;