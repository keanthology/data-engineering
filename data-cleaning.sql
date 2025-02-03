-- For Cost and Storage Optimization --
---- Alter, Delete, Vacuum, Drop ----

%sql
set spark.databricks.delta.retentionDurationCheck.enabled = false

-- FOR LISTING ALL THE TABLES UNDER A CATALOG AND DATABSE --
-- Set the catalog and database
USE CATALOG datahub_dev;
USE dd_datamart;

-- List all the tables in the "adhoc" database
SHOW TABLES;

---------------------------------------------------------------------------------

%sql
-- Views last updated in the past 9 months (Jan 1, 2024 to Sept. 30, 2024)
CREATE OR REPLACE VIEW datahub_dev.dd_datamart.view_last_updated_views AS
SELECT 
    table_catalog AS database_name,
    table_schema AS schema_name,
    table_name,
    table_owner,
    last_altered,
    last_altered_by,
    created
FROM datahub_dev.information_schema.tables 
WHERE TO_DATE(last_altered) BETWEEN '2024-01-01' AND '2024-09-30'
    AND table_type = 'VIEW'
    AND table_schema IN ('dd_datamart')
ORDER BY 
    schema_name,
    last_altered,
    table_name;

-------------------------------------------------------------------------------

%sql
-- Tables last updated in the past 9 months (Jan 1, 2024 to Sept. 30, 2024)
SELECT 
    table_catalog AS database_name,
    table_schema AS schema_name,
    table_name,
    table_owner,
    last_altered,
    last_altered_by,
    created
FROM datahub_dev.information_schema.tables 
WHERE TO_DATE(last_altered) BETWEEN '2024-01-01' AND '2024-09-30'
    AND table_type = 'MANAGED'
    AND  table_schema IN ('dd_datamart')
ORDER BY 
    schema_name,
    last_altered,
    table_name
    ;

--------------------------------------------------------------------------------

%sql
ALTER TABLE datahub.adhoc.hachi_gr_anniv_whitelist_transactions RENAME TO datahub.adhoc.z_hachi_gr_anniv_whitelist_transactions;
ALTER TABLE datahub_dev.dd_datamart.milk_oil_customer_segment RENAME TO datahub_dev.dd_datamart.z_milk_oil_customer_segment;
ALTER TABLE datahub_dev.dd_datamart.milk_oil_customer_segment_top_20 RENAME TO datahub_dev.dd_datamart.z_milk_oil_customer_segment_top_20;
ALTER TABLE datahub_dev.dd_datamart.milk_oil_member_location RENAME TO datahub_dev.dd_datamart.z_milk_oil_member_location;
ALTER TABLE datahub_dev.dd_datamart.milk_oil_total_rsc_transaction_table RENAME TO datahub_dev.dd_datamart.z_milk_oil_total_rsc_transaction_table;
ALTER TABLE datahub_dev.dd_datamart.total_rsc_transaction_table RENAME TO datahub_dev.dd_datamart.z_total_rsc_transaction_table;
ALTER TABLE datahub_dev.dd_datamart.milk_oil_persona_db RENAME TO datahub_dev.dd_datamart.z_milk_oil_persona_db;

DELETE FROM datahub.adhoc.z_hachi_gr_anniv_whitelist_transactions;
DELETE FROM datahub_dev.dd_datamart.z_milk_oil_customer_segment;
DELETE FROM datahub_dev.dd_datamart.z_milk_oil_customer_segment_top_20;
DELETE FROM datahub_dev.dd_datamart.z_milk_oil_member_location;
DELETE FROM datahub_dev.dd_datamart.z_milk_oil_total_rsc_transaction_table;
DELETE FROM datahub_dev.dd_datamart.z_total_rsc_transaction_table;
DELETE FROM datahub_dev.dd_datamart.z_milk_oil_persona_db;

VACUUM datahub.adhoc.z_hachi_gr_anniv_whitelist_transactions RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_milk_oil_customer_segment RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_milk_oil_customer_segment_top_20 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_milk_oil_member_location RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_milk_oil_total_rsc_transaction_table RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_total_rsc_transaction_table RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_milk_oil_persona_db RETAIN 0 HOURS;

DROP TABLE IF EXISTS datahub.adhoc.z_hachi_gr_anniv_whitelist_transactions;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_milk_oil_customer_segment;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_milk_oil_customer_segment_top_20;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_milk_oil_member_location;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_milk_oil_total_rsc_transaction_table;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_total_rsc_transaction_table;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_milk_oil_persona_db;

--------------------------------------------------------------------------------------------------------------------------

%sql

-- RENAME --
-- ADDED LETTER 'Z' AS A PREFIX IN THE TABLE NAMES SO THAT IT WILL BE AT THE LAST PART [ALPHABETICAL]

-- DELETE --
-- FOR DELETING RECORDS MISMO OF THE TABLE

DELETE FROM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_smkt_2024_q1;
DELETE FROM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_diy_2024_q1;
DELETE FROM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_shop_2024_q1;
DELETE FROM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_ssd_2024_q1;
DELETE FROM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_smkt_2024_q2;
DELETE FROM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_diy_2024_q2;
DELETE FROM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_shop_2024_q2;
DELETE FROM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_ssd_2024_q2;
DELETE FROM datahub_dev.dd_datamart.z_rlf_davi_transaction_points_vw;
DELETE FROM datahub_dev.dd_datamart.z_rlf_davi_flight_combined_vw;
DELETE FROM datahub_dev.dd_datamart.z_rlf_davi_transaction_compiled_vw;
DELETE FROM datahub_dev.dd_datamart.z_rlf_davi_transaction_tender_vw;
DELETE FROM datahub_dev.dd_datamart.z_rlf_davi_cobrand_ubp_member_vw;
DELETE FROM datahub_dev.dd_datamart.z_rlf_davi_transaction_clean1_20240430;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_smkt;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_topics_smkt;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_topic14_topmatch;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_topic14_deets;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_topic14_topmatch_product_level;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_topic14_deets_product_level;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_stg_bkp;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_catalog_bkp;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_validate_bkp;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_stg_bkp2;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_catalog_bkp2;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_validate_bkp2;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_stg_bkp3;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_catalog_bkp3;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_validate_bkp3;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_stg_bkp4;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_catalog_bkp4;
DELETE FROM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_validate_bkp4;

-- VACUUM --
-- WIPE OUT DATA FROM THE STORAGE [PARQUET FILES] --
VACUUM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_smkt_2024_q1 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_diy_2024_q1 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_shop_2024_q1 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_ssd_2024_q1 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_smkt_2024_q2 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_diy_2024_q2 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_shop_2024_q2 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_gr_retail_persona_ssd_2024_q2 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_davi_transaction_points_vw RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_davi_flight_combined_vw RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_davi_transaction_compiled_vw RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_davi_transaction_tender_vw RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_davi_cobrand_ubp_member_vw RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_davi_transaction_clean1_20240430 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_smkt RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_topics_smkt RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_topic14_topmatch RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_topic14_deets RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_topic14_topmatch_product_level RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_topic14_deets_product_level RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_stg_bkp RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_catalog_bkp RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_validate_bkp RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_stg_bkp2 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_catalog_bkp2 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_validate_bkp2 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_stg_bkp3 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_catalog_bkp3 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_validate_bkp3 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_stg_bkp4 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_catalog_bkp4 RETAIN 0 HOURS;
VACUUM datahub_dev.dd_datamart.z_rlf_recsys_poc_als_validate_bkp4 RETAIN 0 HOURS;

-- DROP --
-- DELETING TABLES FROM THE DATABASE/SCHEMA --
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_gr_retail_persona_smkt_2024_q1;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_gr_retail_persona_diy_2024_q1;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_gr_retail_persona_shop_2024_q1;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_gr_retail_persona_ssd_2024_q1;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_gr_retail_persona_smkt_2024_q2;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_gr_retail_persona_diy_2024_q2;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_gr_retail_persona_shop_2024_q2;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_gr_retail_persona_ssd_2024_q2;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_davi_transaction_points_vw;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_davi_flight_combined_vw;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_davi_transaction_compiled_vw;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_davi_transaction_tender_vw;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_davi_cobrand_ubp_member_vw;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_davi_transaction_clean1_20240430;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_smkt;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_topics_smkt;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_topic14_topmatch;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_topic14_deets;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_topic14_topmatch_product_level;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_topic14_deets_product_level;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_als_stg_bkp;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_als_catalog_bkp;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_als_validate_bkp;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_als_stg_bkp2;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_als_catalog_bkp2;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_als_validate_bkp2;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_als_stg_bkp3;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_als_catalog_bkp3;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_als_validate_bkp3;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_als_stg_bkp4;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_als_catalog_bkp4;
DROP TABLE IF EXISTS datahub_dev.dd_datamart.z_rlf_recsys_poc_als_validate_bkp4;

---------------------------------------------------------------------------------------------------------------

%sql

-- DROP VIEW --
-- VIEW DOESN'T TAKE UP STORAGE THAT'S WHY WE USE DROP VIEW ONLY --

DROP VIEW IF EXISTS datahub_dev.adhoc.datahub_table_partitions_vw;
DROP VIEW IF EXISTS datahub_dev.adhoc.gr_2022_transactions_line_vw;
DROP VIEW IF EXISTS datahub_dev.adhoc.gr_card_prefix_matrix_master_vw;
DROP VIEW IF EXISTS datahub_dev.adhoc.gr_currentyeartransactions_hq_vw;
DROP VIEW IF EXISTS datahub_dev.adhoc.gr_currentyeartransactions_line_vw;
DROP VIEW IF EXISTS datahub_dev.adhoc.gr_transactions_2022_hq_vw;
DROP VIEW IF EXISTS datahub_dev.adhoc.jmm_kpidashboard_currentyeartransactions_cdp_v2_vw;
DROP VIEW IF EXISTS datahub_dev.adhoc.jmm_kpidashboard_transactions_2022_cdp_v2_vw;
DROP VIEW IF EXISTS datahub_dev.adhoc.jmm_mobile_app_base_vw;

------------------------------------------------------------------------------------------------------------

%python

# SHOW TABLE HISTORY

# Define the table location and path using Unity Catalog
catalog_name = "datahub_dev"
database_name = "dd_datamart"
table_name = "exposed_test"

# Construct the full path for the Delta table using Unity Catalog
delta_table_path = f"{catalog_name}.{database_name}.{table_name}"

# Get the history of the Delta table
table_history_df = spark.sql(f"DESCRIBE HISTORY {delta_table_path}")

# Show the table history (optional)
display(table_history_df)

# Optionally, you can save the history into a table for future analysis
# You can write the history to a separate Delta table for auditing purposes
history_table_name = "table_history_logs"

# Save the history to a Delta table (if it doesn't exist)
table_history_df.write.format("delta").mode("append").saveAsTable(history_table_name)

# Alternatively, save it as a CSV or Parquet file if you prefer
# table_history_df.write.format("csv").save("/path/to/save/history.csv")

---------------------------------------------------------------------------------------------------------

%python
# Define the view and schema details
catalog_name = "datahub_dev"
database_name = "dd_datamart"
view_name = "exposed_test_mv"  # Replace with your materialized view name

# Construct the full path for the view using Unity Catalog
view_path = f"{catalog_name}.{database_name}.{view_name}"

# Query to get metadata about the materialized view
view_metadata_df = spark.sql(f"""
SELECT 
    table_catalog AS database_name,
    table_schema AS schema_name,
    table_name AS view_name,
    table_owner,
    created,
    last_altered
FROM datahub_dev.information_schema.tables
WHERE table_type = 'VIEW'
    AND table_schema = '{database_name}'
    AND table_name = '{view_name}'
""")

# Show the metadata
display(view_metadata_df)

# Query to get the materialized view's SQL definition

# Display the SQL definition
print("Materialized View SQL Definition:")
print(view_definition)

-------------------------------------------------------------------------------------------

# Define the view and schema details

# VIEW TABLE

catalog_name = "datahub_dev"
database_name = "dd_datamart"
view_name = "exposed_test"

# Construct the full path for the view using Unity Catalog
view_path = f"{catalog_name}.{database_name}.{view_name}"

# Query to get metadata about the view
view_metadata_df = spark.sql(f"""
SELECT 
    table_catalog AS database_name,
    table_schema AS schema_name,
    table_name AS view_name,
    table_owner,
    created,
    last_altered
FROM datahub_dev.information_schema.tables
WHERE table_type = 'VIEW'
    AND table_schema = '{database_name}'
    AND table_name = '{view_name}'
""")

# Show the metadata
display(view_metadata_df)

# Query to get the view's SQL definition
view_definition = spark.sql(f"DESCRIBE FORMATTED {view_path}").collect()[0][0]

# Display the SQL definition
print("View SQL Definition:")
print(view_definition)

-------------------------------------------------------------------------------------------

%python

# VIEW ONLY [WITH TABLE HISTORY]

from pyspark.sql.functions import lit

# Define the view and schema details
catalog_name = "datahub_dev"
database_name = "dd_datamart"
view_name = "exposed_test"

# Construct the full path for the view using Unity Catalog
view_path = f"{catalog_name}.{database_name}.{view_name}"

# Query to get metadata about the view
view_metadata_df = spark.sql(f"""
SELECT 
    table_catalog AS database_name,
    table_schema AS schema_name,
    table_name AS view_name,
    table_owner,
    created,
    last_altered
FROM datahub_dev.information_schema.tables
WHERE table_type = 'VIEW'
    AND table_schema = '{database_name}'
    AND table_name = '{view_name}'
""")

# Show the metadata
display(view_metadata_df)

# Query to get the view's SQL definition
view_definition = spark.sql(f"SHOW CREATE TABLE {view_path}").collect()[0][0]

# Display the SQL definition
print("View SQL Definition:")
print(view_definition)

# Get the history of the view
view_history_df = spark.sql(f"DESCRIBE HISTORY {view_path}")

# Show the view history
display(view_history_df)
