use database SUPERSTORE;
use schema staging_layer;

// test CUSTOMER 
alter task INSERT_CUSTOMER_TASK suspend;
alter task MERGE_CUSTOMER_TASK suspend;
alter task DELETE_CUSTOMER_TASK suspend;
show tasks;

-- drop task DELETE_CUSTOMER_TASK;
-- drop task MERGE_CUSTOMER_TASK;

/* 
--------------------------------------------------------
DELETE ALL DATA 
--------------------------------------------------------
*/
delete from SUPERSTORE.HISTORY_LAYER.HISTORY_CUSTOMER;
delete from SUPERSTORE.STAGING_LAYER.STAGING_CUSTOMER;
delete from SUPERSTORE.LANDING_LAYER.LANDING_CUSTOMER;


insert into SUPERSTORE.LANDING_LAYER.LANDING_CUSTOMER
select * from SUPERSTORE.PUBLIC.CUSTOMER;




// test CUSTOMER 
alter task DELETE_CUSTOMER_TASK resume;
alter task MERGE_CUSTOMER_TASK resume;
alter task INSERT_CUSTOMER_TASK resume;

show tasks;

EXECUTE TASK INSERT_CUSTOMER_TASK;


select * from table (INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'INSERT_CUSTOMER_TASK')) order by SCHEDULED_TIME desc;
select * from table (INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'MERGE_CUSTOMER_TASK')) order by SCHEDULED_TIME desc;

--------------------------------------------------------
--------------------------------------------------------
--------------------------------------------------------
----------------------WAIT 1 MINUTE---------------------
--------------------------------------------------------
--------------------------------------------------------
--------------------------------------------------------


-- check the result
select * from SUPERSTORE.STAGING_LAYER.STAGING_CUSTOMER;


select * from CUSTOMER_STREAM;

select * from SUPERSTORE.HISTORY_LAYER.HISTORY_CUSTOMER;


-- call SP_MERGE_CUSTOMER_HISTORY();


select * from SUPERSTORE.LANDING_LAYER.LANDING_CUSTOMER;

insert into SUPERSTORE.LANDING_LAYER.LANDING_CUSTOMER (
    CUSTOMER_ID,
    CUSTOMER_NAME
) values (
    'CG-12520',
    'Tuyet'
);


select * from SUPERSTORE.STAGING_LAYER.STAGING_CUSTOMER where CUSTOMER_NAME='Tuyet';
select * from SUPERSTORE.HISTORY_LAYER.HISTORY_CUSTOMER where CUSTOMER_NAME='Tuyet';


select * from SUPERSTORE.STAGING_LAYER.STAGING_CUSTOMER where CUSTOMER_ID = 'CG-12520';
select * from SUPERSTORE.HISTORY_LAYER.HISTORY_CUSTOMER where CUSTOMER_ID = 'CG-12520';
