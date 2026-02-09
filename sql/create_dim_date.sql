DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date AS
SELECT DISTINCT
    invoice_date::date     AS date,
    EXTRACT(YEAR FROM invoice_date)  AS year,
    EXTRACT(MONTH FROM invoice_date) AS month,
    EXTRACT(DAY FROM invoice_date)   AS day
FROM online_retail;
