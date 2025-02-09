CREATE OR REPLACE EXTERNAL TABLE `terraform-demo-448901.zoomcamp.external_yellow_tripdata_hw`
OPTIONS (
  format = 'PARQUET',
  uris = [
        'gs://your-name-kestra-tsedo/yellow_tripdata_2024-*.parquet' 
          ]
);

CREATE OR REPLACE TABLE terraform-demo-448901.zoomcamp.yellow_tripdata_non_partitioned_hw AS
SELECT * FROM terraform-demo-448901.zoomcamp.external_yellow_tripdata_hw;

Question 1: 
What is count of records for the 2024 Yellow Taxi Data?
SELECT COUNT(1) FROM terraform-demo-448901.zoomcamp.yellow_tripdata_non_partitioned_hw;

Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

SELECT DISTINCT(PULocationID) FROM terraform-demo-448901.zoomcamp.external_yellow_tripdata_hw;  0 
SELECT DISTINCT(PULocationID) FROM terraform-demo-448901.zoomcamp.yellow_tripdata_non_partitioned_hw;  155.12

Question 3:
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

Question 4:
How many records have a fare_amount of 0?
SELECT count(1) FROM terraform-demo-448901.zoomcamp.yellow_tripdata_non_partitioned_hw
WHERE fare_amount = 0;

Question 5:
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
CREATE OR REPLACE TABLE terraform-demo-448901.zoomcamp.yellow_tripdata_partitioned_clustered_hw
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM terraform-demo-448901.zoomcamp.external_yellow_tripdata_hw;

Question 6:
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
SELECT DISTINCT(VendorID) FROM terraform-demo-448901.zoomcamp.yellow_tripdata_non_partitioned_hw
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
;
-- 310.24
SELECT DISTINCT(VendorID) FROM terraform-demo-448901.zoomcamp.yellow_tripdata_partitioned_clustered_hw
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
;
-- 26.84

Question 7:
Where is the data stored in the External Table you created?
GCP Bucket

Question 8:
It is best practice in Big Query to always cluster your data:
False

