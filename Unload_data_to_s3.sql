--Create FILE FORMAT
CREATE OR REPLACE FILE FORMAT my_csv_unload_format
type = csv field_delimiter = ',' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true compression = gzip;


--Set the Integration Object
ALTER STORAGE INTEGRATION s3_int SET storage_allowed_locations = ('s3://knoldus/employee/', 's3://knoldus/emp_unload/')

DESC INTEGRATION s3_int;


--Create External Staging Area where data will be unloaded
CREATE OR REPLACE STAGE my_s3_unload_stage
  storage_integration = s3_int
  url = 's3://knoldus/emp_unload/'
  file_format = my_csv_unload_format;
  
  
--Checking the data from table
SELECT * FROM tbl_empl LIMIT 30;


--Copying data from Snowflake to External Staging Area
COPY INTO @my_s3_unload_stage FROM tbl_empl;


--Comparing data from table and external staging area
SELECT COUNT(*) FROM tbl_empl;
SELECT COUNT(*) FROM @my_s3_unload_stage;


COPY INTO @my_s3_unload_stage/select_
FROM
(
  SELECT 
  first_name,
  email 
  FROM
  tbl_empl
)

--Unload as Parquet format
COPY INTO @my_s3_unload_stage/parquet_
FROM tbl_empl
FILE_FORMAT=(TYPE='PARQUET' SNAPPY_COMPRESSION=TRUE)
