/* 
--------------------------------------------------------
                    LANDING-LAYER
        - create STORAGE INTEGRATION 
        - create STAGE
        - copy data from STAGE into LANDING-LAYER


                    DO NOT RUN ALL
                    
    (shouldn't create or replace storage integration 
    + cannot create storage integration again)
--------------------------------------------------------
*/

use role accountadmin; 
use database SUPERSTORE;
use schema LANDING_LAYER;


-- Create a cloud storage integration in Snowflake
create storage integration s3_snowie_data
    type = external_stage
    storage_provider = 'S3'
    enabled = TRUE
    storage_aws_role_arn = 'arn:aws:iam::449143050604:role/snowflake-snowie-role'
    storage_allowed_locations = ('s3://snowie-snowflake-bucket/');

/* 
--------------------------------------------------------
                    RUN FROM HERE to test, 
        already contained in manage-pipeline file
--------------------------------------------------------
*/
desc integration s3_snowie_data;

create or replace stage aws_ext_stage_integration
    url = 's3://snowie-snowflake-bucket'
    storage_integration = s3_snowie_data;

list @aws_ext_stage_integration;

delete from SUPERSTORE.LANDING_LAYER.LANDING_PRODUCT;
delete from SUPERSTORE.LANDING_LAYER.LANDING_SHIPMENT;

copy into SUPERSTORE.LANDING_LAYER.LANDING_PRODUCT
from @aws_ext_stage_integration/product.csv
file_format = SUPERSTORE.LANDING_LAYER.CSV_FILE_FORMAT
on_error = abort_statement
force=true;


copy into SUPERSTORE.LANDING_LAYER.LANDING_SHIPMENT
from @aws_ext_stage_integration/shipment.csv
file_format = SUPERSTORE.LANDING_LAYER.CSV_FILE_FORMAT
on_error = abort_statement
force=true;


select * from SUPERSTORE.LANDING_LAYER.LANDING_PRODUCT;
select * from SUPERSTORE.LANDING_LAYER.LANDING_SHIPMENT;


/* 
--------------------------------------------------------
So now, LANDING_PRODUCT and LANDING_SHIPMENT has data. 
However, other tables in LANDING-LAYER don't.
--------------------------------------------------------
*/
