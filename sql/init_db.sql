-- Create Gold Schema
CREATE SCHEMA IF NOT EXISTS gold;

-- Dimension Tables
CREATE TABLE IF NOT EXISTS gold.dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(5),
    customer_lat FLOAT,
    customer_lng FLOAT
);

-- Dimension Products
CREATE TABLE IF NOT EXISTS gold.dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name_english VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

-- Dimension Sellers
CREATE TABLE IF NOT EXISTS gold.dim_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(5),
    seller_lat FLOAT,
    seller_lng FLOAT
);

-- Fact Order Items
CREATE TABLE IF NOT EXISTS gold.fct_order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50) REFERENCES gold.dim_products(product_id),
    seller_id VARCHAR(50) REFERENCES gold.dim_sellers(seller_id),
    customer_id VARCHAR(50) REFERENCES gold.dim_customers(customer_id),
    shipping_limit_date TIMESTAMP,
    price FLOAT,
    freight_value FLOAT,
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    PRIMARY KEY (order_id, order_item_id)
);

-- Fact Order Payments
CREATE TABLE IF NOT EXISTS gold.fct_order_payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    customer_id VARCHAR(50) REFERENCES gold.dim_customers(customer_id),
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value FLOAT,
    PRIMARY KEY (order_id, payment_sequential)
);

-- Fact Order Reviews
CREATE TABLE IF NOT EXISTS gold.fct_order_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    customer_id VARCHAR(50) REFERENCES gold.dim_customers(customer_id),
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    PRIMARY KEY (review_id, order_id)
);

-- Indexes for performance (Star Schema optimization)
CREATE INDEX IF NOT EXISTS idx_fct_order_items_product ON gold.fct_order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_fct_order_items_seller ON gold.fct_order_items(seller_id);
CREATE INDEX IF NOT EXISTS idx_fct_order_items_customer ON gold.fct_order_items(customer_id);
CREATE INDEX IF NOT EXISTS idx_fct_order_items_status ON gold.fct_order_items(order_status);
CREATE INDEX IF NOT EXISTS idx_fct_order_items_purchase ON gold.fct_order_items(order_purchase_timestamp);

CREATE INDEX IF NOT EXISTS idx_fct_order_payments_customer ON gold.fct_order_payments(customer_id);
CREATE INDEX IF NOT EXISTS idx_fct_order_payments_order ON gold.fct_order_payments(order_id);

CREATE INDEX IF NOT EXISTS idx_fct_order_reviews_order ON gold.fct_order_reviews(order_id);
CREATE INDEX IF NOT EXISTS idx_fct_order_reviews_customer ON gold.fct_order_reviews(customer_id);

-- DLQ Table for tracking bad records
CREATE SCHEMA IF NOT EXISTS logging;
CREATE TABLE IF NOT EXISTS logging.dead_letter_queue (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(50),
    rejected_record JSONB,
    rejection_reason TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);