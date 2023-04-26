SELECT modelid, name, options_color, options_size, count(modelid) count_model
FROM `data-engineering-poc-383207.shopee_dataset.iphone_data`
GROUP BY modelid, name, options_color, options_size
ORDER BY count_model DESC