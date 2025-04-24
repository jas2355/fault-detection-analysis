--Step 1: check whether the data can extract efficiently or not
SELECT * 
FROM `database.tool_sensor_data_cleaned.tool sensor data` 
LIMIT 10

--Step 2: Querying & Filtering data
--1. odered by time
SELECT *
FROM `database.tool_sensor_data_cleaned.tool sensor data` 
WHERE ToolName = 'A' 
ORDER BY new_timestamp

--2. water lot info
SELECT 
  Wafer_ID,
  Lot_ID, 
  ToolName, 
  new_timestamp, 
  OunhHslCRwIRilo, 
  BmpcKiosIw
FROM `database.tool_sensor_data_cleaned.tool sensor data`
ORDER BY 
  Wafer_ID, 
  Lot_ID, 
  new_timestamp

--Step 3: Data Cleaning
SELECT *
FROM `database.tool_sensor_data_cleaned.tool sensor data`
WHERE OunhHslCRwIRilo IS NOT NULL

--Step 4: Exploratory Data Analysis (EDA)
--Standardization
WITH sensor_data AS (
  SELECT
    OunhHslCRwIRilo,
    BmpcKiosIw
  FROM `database.tool_sensor_data_cleaned.tool sensor data`
),
statistics AS (
  SELECT
    AVG(OunhHslCRwIRilo) AS avg_OunhHslCRwIRilo,
    STDDEV(OunhHslCRwIRilo) AS std_OunhHslCRwIRilo,
    AVG(BmpcKiosIw) AS avg_BmpcKiosIw,
    STDDEV(BmpcKiosIw) AS std_BmpcKiosIw
  FROM sensor_data
),
standardized_data AS (
  SELECT
    (OunhHslCRwIRilo - avg_OunhHslCRwIRilo) / std_OunhHslCRwIRilo AS z_OunhHslCRwIRilo,
    (BmpcKiosIw - avg_BmpcKiosIw) / std_BmpcKiosIw AS z_BmpcKiosIw
  FROM sensor_data, statistics
)

-- Calculate summary statistics from standardized data
SELECT
  'min' AS statistic,
  MIN(z_OunhHslCRwIRilo) AS OunhHslCRwIRilo,
  MIN(z_BmpcKiosIw) AS BmpcKiosIw
FROM standardized_data

UNION ALL

SELECT
  'max' AS statistic,
  MAX(z_OunhHslCRwIRilo) AS OunhHslCRwIRilo,
  MAX(z_BmpcKiosIw) AS BmpcKiosIw
FROM standardized_data

UNION ALL

SELECT
  'median' AS statistic,
  APPROX_QUANTILES(z_OunhHslCRwIRilo, 2)[OFFSET(1)] AS OunhHslCRwIRilo,
  APPROX_QUANTILES(z_BmpcKiosIw, 2)[OFFSET(1)] AS BmpcKiosIw
FROM standardized_data

UNION ALL

SELECT
  'count' AS statistic,
  COUNT(z_OunhHslCRwIRilo) AS OunhHslCRwIRilo,
  COUNT(z_BmpcKiosIw) AS BmpcKiosIw
FROM standardized_data

--Histogram
SELECT
  FLOOR(OunhHslCRwIRilo / 10) * 10 AS bin_OunhHslCRwIRilo,
  COUNT(*) AS count_OunhHslCRwIRilo,
  FLOOR(BmpcKiosIw / 10) * 10 AS bin_BmpcKiosIw,
  COUNT(*) AS count_BmpcKiosIw
FROM
  `database.tool_sensor_data_cleaned.tool sensor data`
GROUP BY
  bin_OunhHslCRwIRilo,
  bin_BmpcKiosIw
ORDER BY
  bin_OunhHslCRwIRilo,
  bin_BmpcKiosIw

--Time series (line graph)
SELECT 
  new_timestamp, 
  ToolName, 
  AVG(OunhHslCRwIRilo) as avg_OunhHslCRwIRilo
FROM `database.tool_sensor_data_cleaned.tool sensor data`
GROUP BY 
  ToolName, 
  new_timestamp
ORDER BY new_timestamp

--Correlation
SELECT 
  CORR(OunhHslCRwIRilo, BmpcKiosIw) as OunhHslCRwIRilo_BmpcKiosIw_corr,
  CORR(OunhHslCRwIRilo, SwpYipezsdueC) as OunhHslCRwIRilo_SwpYipezsdueC_corr,
  CORR(BmpcKiosIw, SwpYipezsdueC) AS BmpcKiosIw_SwpYipezsdueC_corr
FROM `database.tool_sensor_data_cleaned.tool sensor data`
