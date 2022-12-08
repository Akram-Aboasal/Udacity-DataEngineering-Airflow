# Project: Data Pipelines with Airflow

## Scenario

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Data Pipeline

For this project we will use Apache Airflow to create a pipeline to extract, transform, and load JSON data from Amazon S3 to Amazon Redshift.

## Airflow Custom Operators

### Stage to Redshift Operator

This operator will load JSON data from Amazon S3 into staging tables in Amazon Redshift.

The staging operation is based on parameters passed to the operator:

* redshift_conn_id - The connection ID of the Amazon Redshift connection configured in Apache Airflow. Defaults to 'redshift'
* aws_credentials_id - The connection ID of the credentials to connect to Amazon S3 configured in Apache Airflow. Defaults to 'aws_credentials'
* table_name - The staging table name where data will be copied.
* s3_bucket - The Amazon S3 bucket where data will be copied from.
* json_path - The JSON path parameter used during Redshift copy operation. Defaults to 'auto'
* use_partitioning - If set to True, the S3 data will be copied based on year and month partitions based on the logical execution date of the DAG
* execution_date - Templated parameter for the logical execution date of the DAG
* truncate_table - If True, the staging table will be cleared prior to copy.

### Load Dimension Table Operator

This operator will transform and load data from staging tables to the dimension tables.

This operator has the following parameters:

* redshift_conn_id - The connection ID of the Amazon Redshift connection configured in Apache Airflow. Defaults to 'redshift'
* dimension_table_name - The table name of the dimension table where data will be inserted.
* dimension_insert_columns -  The columns of the dimension table where data will be inserted.
* dimension_insert_sql - The SQL statement that will transform the data from the staging table(s) to the proper format for inserting into the dimension table.
* truncate_table - If True, the dimension table will be cleared before inserting new data.

### Load Fact Table Operator

This operator will transform and load data from staging tables to the fact tables.

This operator has the following parameters:

* redshift_conn_id - The connection ID of the Amazon Redshift connection configured in Apache Airflow. Defaults to 'redshift'
* fact_table_name - The table name of the fact table where data will be inserted.
* fact_insert_columns -  The columns of the fact table where data will be inserted.
* fact_insert_sql - The SQL statement that will transform the data from the staging table(s) to the proper format for inserting into the fact table.
* truncate_table - If True, the fact table will be cleared before inserting new data.

### Data Quality Operator

This operator allows for data quality checks against the data once the ETL process has completed.

This operator has the folloing parameters:

* redshift_conn_id - The connection ID of the Amazon Redshift connection configured in Apache Airflow. Defaults to 'redshift'
* sql_check_queries - List of SQL statements that correspond to data quality checks to be performed.
* expected_results - List of lambda expression used as predicates to validate the number of rows returned from the data quality check SQL statements

i.e. You may have a SQL statement:

```sql
 SELECT COUNT(*) FROM songs WHERE songid IS NULL
 ```

with a corresponding check to ensure zero rows are returned where song ID is NULL

```python
lambda num_rows: num_rows==0
```

### Pipeline Diagram

![Pipeline Diagram](pipeline.png)

## Setup Airflow

1. Copy dags folder into your airflow dags.
1. Copy plugins folder to your airflow plugins for custom operators.

Airflow will need two connections:

* redshift - A PostgreSQL connection with credentials to Amazon Redshift
* aws_credentials - Amazon Web Services connection with cr4edentials to access S3.

## Setup Database

In order to create the tables in Redshift, I have included a DAG setup_dag.py that will create the necessary tables.

You may wish to edit the etl_dag.py DAG to alter the parameters like start_date and scheduled_interval.

## Execution

Enable the DAG in Airflow and it should begin processing.
