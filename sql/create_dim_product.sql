DROP TABLE IF EXISTS dim_product;

CREATE TABLE dim_product AS
SELECT DISTINCT
    stock_code      AS product_id,
    description
FROM online_retail
WHERE stock_code IS NOT NULL;
