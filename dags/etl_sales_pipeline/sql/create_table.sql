CREATE TABLE IF NOT EXISTS sales_clean (
    order_id INT,
    customer_id INT,
    amount DECIMAL(10,2),
    order_date DATE
)
DISTSTYLE KEY
DISTKEY(order_id)
SORTKEY(order_date);
