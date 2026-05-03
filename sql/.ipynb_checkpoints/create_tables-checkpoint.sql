DROP TABLE IF EXISTS events CASCADE;

CREATE TABLE IF NOT EXISTS events (
    event_id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR(20) NOT NULL CHECK (event_type IN ('view', 'cart', 'remove_from_cart', 'purchase')),
    product_id BIGINT NOT NULL,
    category_id BIGINT NOT NULL,
    category_code VARCHAR(255),
    brand VARCHAR(100),
    price DECIMAL(10, 2),
    user_id BIGINT NOT NULL,
    user_session VARCHAR(100) NOT NULL
);