/* 
--------------------------------------------------------
MAKE SURE that:
- 1-Landing-layer has been run (created 3 schemas, and tables in LANDING-LAYER)
- 2-Staging-layer has been run (created tables in STAGING-LAYER, STREAMS and TASKS)
- 3-History-layer has been run (created tables in HISTORY-LAYER)

In this file, we will:
- suspend all tasks (including INSERT, MERGE and DELETE)
- delete all data in 3 layers
- insert data into LANDING-LAYER from PUBLIC 
--------------------------------------------------------
*/

use database SUPERSTORE;
use schema staging_layer;

/* 
--------------------------------------------------------
SUSPEND ALL TASKS
--------------------------------------------------------
*/
-- suspend task insert data 
alter task INSERT_CUSTOMER_TASK suspend;
alter task INSERT_ORDER_INFOR_TASK suspend;
alter task INSERT_ORDER_PRODUCT_TASK suspend;
alter task INSERT_PRODUCT_TASK suspend;
alter task INSERT_SHIPMENT_TASK suspend;

-- SUSPEND TASKS MERGE DATA 
alter task MERGE_CUSTOMER_TASK suspend;
alter task MERGE_ORDER_INFOR_TASK suspend;
alter task MERGE_ORDER_PRODUCT_TASK suspend;
alter task MERGE_PRODUCT_TASK suspend;
alter task MERGE_SHIPMENT_TASK suspend;

-- SUSPEND TASKS DELETE DATA 
alter task DELETE_CUSTOMER_TASK suspend;
alter task DELETE_ORDER_INFOR_TASK suspend;
alter task DELETE_ORDER_PRODUCT_TASK suspend;
alter task DELETE_PRODUCT_TASK suspend;
alter task DELETE_SHIPMENT_TASK suspend;

show tasks;
--------------------------------------------------------


/* 
--------------------------------------------------------
DELETE ALL DATA IN HISTORY
--------------------------------------------------------
*/
delete from SUPERSTORE.HISTORY_LAYER.HISTORY_CUSTOMER;
delete from SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_INFOR;
delete from SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_PRODUCT;
delete from SUPERSTORE.HISTORY_LAYER.HISTORY_PRODUCT;
delete from SUPERSTORE.HISTORY_LAYER.HISTORY_SHIPMENT;

/* 
--------------------------------------------------------
DELETE ALL DATA IN STAGING
--------------------------------------------------------
*/
delete from SUPERSTORE.STAGING_LAYER.STAGING_CUSTOMER;
delete from SUPERSTORE.STAGING_LAYER.STAGING_ORDER_INFOR;
delete from SUPERSTORE.STAGING_LAYER.STAGING_ORDER_PRODUCT;
delete from SUPERSTORE.STAGING_LAYER.STAGING_PRODUCT;
delete from SUPERSTORE.STAGING_LAYER.STAGING_SHIPMENT;

/* 
--------------------------------------------------------
DELETE ALL DATA IN LANDING
--------------------------------------------------------
*/
delete from SUPERSTORE.LANDING_LAYER.LANDING_CUSTOMER;
delete from SUPERSTORE.LANDING_LAYER.LANDING_ORDER_INFOR;
delete from SUPERSTORE.LANDING_LAYER.LANDING_ORDER_PRODUCT;
delete from SUPERSTORE.LANDING_LAYER.LANDING_PRODUCT;
delete from SUPERSTORE.LANDING_LAYER.LANDING_SHIPMENT;

show tables;


/* 
--------------------------------------------------------
For more testing convenient, copy data from public to LANDING
--------------------------------------------------------
*/
insert into SUPERSTORE.LANDING_LAYER.LANDING_CUSTOMER
select * from SUPERSTORE.PUBLIC.CUSTOMER;

insert into SUPERSTORE.LANDING_LAYER.LANDING_ORDER_INFOR
select * from SUPERSTORE.PUBLIC.ORDER_INFOR;

insert into SUPERSTORE.LANDING_LAYER.LANDING_ORDER_PRODUCT
select * from SUPERSTORE.PUBLIC.ORDER_PRODUCT;


/* 
--------------------------------------------------------
COPY DATA from STAGE (external S3 STORAGE) to LANDING
--------------------------------------------------------
*/
create or replace stage aws_ext_stage_integration
    url = 's3://snowie-snowflake-bucket'
    storage_integration = s3_snowie_data;


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





/* 
--------------------------------------------------------
        RESUME TASKS DELETE DATA in LANDING-LAYER
--------------------------------------------------------
*/
alter task DELETE_CUSTOMER_TASK resume;
alter task DELETE_ORDER_INFOR_TASK resume;
alter task DELETE_ORDER_PRODUCT_TASK resume;
alter task DELETE_PRODUCT_TASK resume;
alter task DELETE_SHIPMENT_TASK resume;
----------------------------------------------------------


/* 
--------------------------------------------------------
        RESUME TASKS MERGE DATA in HISTORY-LAYER
--------------------------------------------------------
*/
alter task MERGE_CUSTOMER_TASK resume;
alter task MERGE_ORDER_INFOR_TASK resume;
alter task MERGE_ORDER_PRODUCT_TASK resume;
alter task MERGE_PRODUCT_TASK resume;
alter task MERGE_SHIPMENT_TASK resume;
----------------------------------------------------------


/* 
--------------------------------------------------------
  RESUME TASK TO SEND NEW DATA TO STAGING-LAYER
--------------------------------------------------------
*/
alter task INSERT_CUSTOMER_TASK resume;
alter task INSERT_ORDER_INFOR_TASK resume;
alter task INSERT_ORDER_PRODUCT_TASK resume;
alter task INSERT_PRODUCT_TASK resume;
alter task INSERT_SHIPMENT_TASK resume;

/* 
--------------------------------------------------------
  RUN TASKs IMMEDIATELY
--------------------------------------------------------
*/
EXECUTE TASK INSERT_CUSTOMER_TASK;
EXECUTE TASK INSERT_ORDER_INFOR_TASK;
EXECUTE TASK INSERT_ORDER_PRODUCT_TASK;
EXECUTE TASK INSERT_PRODUCT_TASK;
EXECUTE TASK INSERT_SHIPMENT_TASK;


show tasks;

--------------------------------------------------------
--------------------------------------------------------
--------------------------------------------------------
----------------------WAIT 1 MINUTE---------------------
--------------------------------------------------------
--------------------------------------------------------
--------------------------------------------------------

/* 
--------------------------------------------------------
            CHECK ALL DATA IN STAGING-LAYER
--------------------------------------------------------
*/
-- check the result
select * from SUPERSTORE.STAGING_LAYER.STAGING_CUSTOMER;
select * from SUPERSTORE.STAGING_LAYER.STAGING_ORDER_INFOR;
select * from SUPERSTORE.STAGING_LAYER.STAGING_ORDER_PRODUCT;
select * from SUPERSTORE.STAGING_LAYER.STAGING_PRODUCT;
select * from SUPERSTORE.STAGING_LAYER.STAGING_SHIPMENT;


/* 
--------------------------------------------------------
                 CHECK DATA IN STREAM
--------------------------------------------------------
*/
select * from CUSTOMER_STREAM;
select * from ORDER_INFOR_STREAM;
select * from ORDER_PRODUCT_STREAM;
select * from PRODUCT_STREAM;
select * from SHIPMENT_STREAM;


-- [OPTIONAL] CHECK LOGS OF TASKs 
select * from table (INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'INSERT_CUSTOMER_TASK')) order by SCHEDULED_TIME desc;
select * from table (INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'INSERT_ORDER_INFOR_TASK')) order by SCHEDULED_TIME desc;
select * from table (INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'INSERT_ORDER_PRODUCT_TASK')) order by SCHEDULED_TIME desc;
select * from table (INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'INSERT_PRODUCT_TASK')) order by SCHEDULED_TIME desc;
select * from table (INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'INSERT_SHIPMENT_TASK')) order by SCHEDULED_TIME desc;



-- [OPTIONAL] CHECK LOGS OF TASKs 
select * from table (INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'MERGE_CUSTOMER_TASK')) order by SCHEDULED_TIME desc;
select * from table (INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'MERGE_ORDER_INFOR_TASK'))  order by SCHEDULED_TIME desc;
select * from table (INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'MERGE_ORDER_PRODUCT_TASK')) order by SCHEDULED_TIME desc;
select * from table (INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'MERGE_PRODUCT_TASK')) order by SCHEDULED_TIME desc;
select * from table (INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'MERGE_SHIPMENT_TASK')) order by SCHEDULED_TIME desc;
----------------------------------------------------------


/* 
--------------------------------------------------------
        CHECK IF DATA HAS BEEN MERGED IN HISTORY
            expect we have all data in history
--------------------------------------------------------
*/
select * from SUPERSTORE.HISTORY_LAYER.HISTORY_CUSTOMER;
select * from SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_INFOR;
select * from SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_PRODUCT;
select * from SUPERSTORE.HISTORY_LAYER.HISTORY_PRODUCT;
select * from SUPERSTORE.HISTORY_LAYER.HISTORY_SHIPMENT;


/* 
--------------------------------------------------------
        CHECK IF DATA HAS BEEN DELETED IN LANDING
                expect no available data 
--------------------------------------------------------
*/
select * from SUPERSTORE.LANDING_LAYER.LANDING_CUSTOMER;
select * from SUPERSTORE.LANDING_LAYER.LANDING_ORDER_INFOR;
select * from SUPERSTORE.LANDING_LAYER.LANDING_ORDER_PRODUCT;
select * from SUPERSTORE.LANDING_LAYER.LANDING_PRODUCT;
select * from SUPERSTORE.LANDING_LAYER.LANDING_SHIPMENT;


select * from SUPERSTORE.STAGING_LAYER.STAGING_CUSTOMER where CUSTOMER_NAME='Tuyet';
select * from SUPERSTORE.HISTORY_LAYER.HISTORY_CUSTOMER where CUSTOMER_NAME='Tuyet';


select * from SUPERSTORE.STAGING_LAYER.STAGING_CUSTOMER where CUSTOMER_ID = 'CG-12520';
select * from SUPERSTORE.HISTORY_LAYER.HISTORY_CUSTOMER where CUSTOMER_ID = 'CG-12520';



select count(*) from  SUPERSTORE.HISTORY_LAYER.HISTORY_CUSTOMER;
select count(*) from  SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_INFOR;
select count(*) from  SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_PRODUCT;
select count(*) from  SUPERSTORE.HISTORY_LAYER.HISTORY_PRODUCT;

select count(*) from  SUPERSTORE.HISTORY_LAYER.HISTORY_SHIPMENT;