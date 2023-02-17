-- create an external table
CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp_us.fhv`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoomcamp-pmnv/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- get count of rows in the table
select COUNT (1) FROM `dtc-de-piminov.dezoomcamp_us.fhv` LIMIT 100

-- get count of distinct Affiliated_base_number
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `dtc-de-piminov.dezoomcamp_us.fhv` LIMIT 1000

-- create an 'internal' table
CREATE OR REPLACE TABLE `dtc-de-piminov.dezoomcamp_us.fhv_non_partitoned`
AS SELECT * FROM `dtc-de-piminov.dezoomcamp_us.fhv`;

-- get count of distinct affiliated_base_number and look at size of processed data 
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `dtc-de-piminov.dezoomcamp_us.fhv_non_partitoned` LIMIT 1000

-- get count of not NULL PUlocationID AND DOlocationID
SELECT COUNT(1)  FROM `dtc-de-piminov.dezoomcamp_us.fhv_non_partitoned`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL
 LIMIT 10

-- create a partitioned and clustered table 
CREATE OR REPLACE TABLE 'dtc-de-piminov.dezoomcamp_us.fhv_partitoned_clustered'
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM dtc-de-piminov.dezoomcamp_us.fhv;


-- evaluate next query on not paritioned and patitioned tables
SELECT count(distinct(Affiliated_base_number)) FROM `dtc-de-piminov.dezoomcamp_us.fhv_non_partitoned`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'
LIMIT 10

SELECT count(distinct(Affiliated_base_number)) FROM `dtc-de-piminov.dezoomcamp_us.fhv_partitoned_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'
LIMIT 10

