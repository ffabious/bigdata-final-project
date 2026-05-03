COPY events(event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session) 
FROM STDIN WITH CSV HEADER DELIMITER ',' NULL AS ''
WHERE event_time IS NOT NULL 
  AND event_type IS NOT NULL 
  AND product_id IS NOT NULL 
  AND category_id IS NOT NULL 
  AND user_id IS NOT NULL 
  AND user_session IS NOT NULL;