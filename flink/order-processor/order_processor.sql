-- Flink SQL Order Processing Job
-- This job processes orders from Kafka and calculates business metrics

-- Create the source table for orders from Kafka
CREATE TABLE orders_source (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    price DOUBLE,
    `timestamp` TIMESTAMP(3),
    region STRING,
    processing_time AS PROCTIME(),
    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-order-processor',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- Create sink table for processed orders (console output)
CREATE TABLE processed_orders_sink (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    price DOUBLE,
    order_value DOUBLE,
    order_category STRING,
    region STRING,
    order_timestamp TIMESTAMP(3),
    processing_latency_ms BIGINT,
    processing_time TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

-- Create sink table for order metrics
CREATE TABLE order_metrics_sink (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    total_orders BIGINT,
    total_revenue DOUBLE,
    avg_order_value DOUBLE,
    high_value_orders BIGINT,
    medium_value_orders BIGINT,
    low_value_orders BIGINT,
    avg_processing_latency_ms DOUBLE,
    orders_per_second DOUBLE
) WITH (
    'connector' = 'print'
);

-- Process orders with business logic
INSERT INTO processed_orders_sink
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    price,
    quantity * price as order_value,
    CASE
        WHEN quantity * price > 1000 THEN 'high_value'
        WHEN quantity * price > 100 THEN 'medium_value'
        ELSE 'low_value'
    END as order_category,
    region,
    `timestamp` as order_timestamp,
    TIMESTAMPDIFF(MILLISECOND, `timestamp`, processing_time) as processing_latency_ms,
    processing_time
FROM orders_source;

-- Calculate windowed metrics (5-second tumbling windows)
INSERT INTO order_metrics_sink
SELECT
    TUMBLE_START(processing_time, INTERVAL '5' SECOND) as window_start,
    TUMBLE_END(processing_time, INTERVAL '5' SECOND) as window_end,
    COUNT(*) as total_orders,
    SUM(quantity * price) as total_revenue,
    AVG(quantity * price) as avg_order_value,
    SUM(CASE WHEN quantity * price > 1000 THEN 1 ELSE 0 END) as high_value_orders,
    SUM(CASE WHEN quantity * price > 100 AND quantity * price <= 1000 THEN 1 ELSE 0 END) as medium_value_orders,
    SUM(CASE WHEN quantity * price <= 100 THEN 1 ELSE 0 END) as low_value_orders,
    AVG(TIMESTAMPDIFF(MILLISECOND, `timestamp`, processing_time)) as avg_processing_latency_ms,
    COUNT(*) / 5.0 as orders_per_second
FROM orders_source
GROUP BY TUMBLE(processing_time, INTERVAL '5' SECOND);