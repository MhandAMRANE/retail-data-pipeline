DROP TABLE IF EXISTS dim_country;

CREATE TABLE dim_country AS
SELECT DISTINCT
    country
FROM online_retail
WHERE country IS NOT NULL;
