CREATE TABLE IF NOT EXISTS sales_clean (
    order_id      VARCHAR(20),
    order_date    DATE,
    customer_id   VARCHAR(20),
    product_id    VARCHAR(20),
    quantity      INT,
    price         DECIMAL(10,2),
    total_amount  DECIMAL(12,2)
)
DISTSTYLE AUTO
SORTKEY(order_date);
