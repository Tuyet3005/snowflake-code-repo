/* 
--------------------------------------------------------
bản ni được duplicated ra từ 2-staging-exploring 
để schedule task depend lẫn nhau 
INSERT --> MERGE --> DELETE
--------------------------------------------------------
*/


/* 
--------------------------------------------------------
                INSERT INTO STAGING
--------------------------------------------------------
*/

-- SELECT DATABASE + SCHEMA 
use database SUPERSTORE;
use schema staging_layer;

/* 
--------------------------------------------------------
  [MANDATORY] CREATE TABLES IN STAGING-LAYER
--------------------------------------------------------
*/
-- STAGING_CUSTOMER
create or replace table staging_customer (
    CUSTOMER_ID varchar(20),
    CUSTOMER_NAME varchar(50)
);

-- STAGING_ORDER_INFOR 
create or replace table staging_order_infor (
    ORDER_ID varchar(20),
    ORDER_DATE date,
    CUSTOMER_ID varchar(20)
);

-- STAGING_ORDER_PRODUCT
create or replace table staging_order_product (
    ID NUMBER(38,0), 
    ORDER_ID VARCHAR(20),
    PRODUCT_ID VARCHAR(20),
    SALES NUMBER(38,4),
    QUANTITY NUMBER(38,0),
    DISCOUNT NUMBER(38,2),
    PROFIT NUMBER(38,4)
);

-- STAGING_PRODUCT 
create or replace table staging_product(
    PRODUCT_ID VARCHAR(20),
    CATEGORY VARCHAR(50),
    SUB_CATEGORY VARCHAR(50),
    PRODUCT_NAME VARCHAR(200)
);

-- STAGING SHIPMENT
create or replace table staging_shipment (
    SHIPMENT_ID NUMBER(38,0), 
    ORDER_ID VARCHAR(16777216), 
    SHIP_DATE DATE,
    SHIP_MODE VARCHAR(16777216), 
    SEGMENT VARCHAR(16777216), 
    COUNTRY VARCHAR(16777216), 
    CITY VARCHAR(16777216), 
    STATE VARCHAR(16777216), 
    POSTAL_CODE NUMBER(38,0), 
    REGION VARCHAR(16777216) 
);
show tables;


/* 
--------------------------------------------------------
  [MANDATORY] USING TASK TO SEND NEW DATA TO STAGING-LAYER
--------------------------------------------------------
*/
-- CUSTOMER
create or replace task INSERT_CUSTOMER_TASK
    warehouse = compute_wh
    schedule = '1 MINUTE'
as 
insert into SUPERSTORE.STAGING_LAYER.STAGING_CUSTOMER (
    CUSTOMER_ID,
    CUSTOMER_NAME
) 
select 
    CUSTOMER_ID,
    INITCAP(CUSTOMER_NAME)
from SUPERSTORE.LANDING_LAYER.LANDING_CUSTOMER lc 
where not exists (
    select *
    from SUPERSTORE.STAGING_LAYER.STAGING_CUSTOMER sc
    where lc.CUSTOMER_ID = sc.CUSTOMER_ID and INITCAP(lc.CUSTOMER_NAME) = sc.CUSTOMER_NAME
);

-- ORDER_INFOR
create or replace task INSERT_ORDER_INFOR_TASK 
    warehouse = compute_wh
    schedule = '1 MINUTE'
as 
insert into SUPERSTORE.STAGING_LAYER.STAGING_ORDER_INFOR (
    ORDER_ID,
    ORDER_DATE,
    CUSTOMER_ID
)
select 
    ORDER_ID,
    ORDER_DATE,
    CUSTOMER_ID
from SUPERSTORE.LANDING_LAYER.LANDING_ORDER_INFOR lo
where not exists (
    select *
    from SUPERSTORE.STAGING_LAYER.STAGING_ORDER_INFOR so
    where lo.ORDER_ID = so.ORDER_ID 
    and lo.order_date = so.order_date
    and lo.customer_id = so.customer_id
);

-- ORDER_PRODUCT
create or replace task INSERT_ORDER_PRODUCT_TASK
    warehouse = compute_wh
    schedule = '1 MINUTE'
as 
insert into SUPERSTORE.STAGING_LAYER.STAGING_ORDER_PRODUCT (
    ID, 
    ORDER_ID,
    PRODUCT_ID,
    SALES,
    QUANTITY,
    DISCOUNT,
    PROFIT
)
select 
    ID, 
    ORDER_ID,
    PRODUCT_ID,
    SALES,
    QUANTITY,
    DISCOUNT,
    PROFIT
from SUPERSTORE.LANDING_LAYER.LANDING_ORDER_PRODUCT lo 
where not exists (
    select *
    from SUPERSTORE.STAGING_LAYER.STAGING_ORDER_PRODUCT so 
    where lo.ID = so.ID 
    and lo.ORDER_ID = so.ORDER_ID
    and lo.PRODUCT_ID = so.PRODUCT_ID
    and lo.SALES = so.SALES
    and lo.QUANTITY = so.QUANTITY
    and lo.DISCOUNT = so.DISCOUNT
    and lo.PROFIT = so.PROFIT 
);

-- PRODUCT 
create or replace task INSERT_PRODUCT_TASK 
    warehouse = compute_wh
    schedule = '1 MINUTE'
as
insert into SUPERSTORE.STAGING_LAYER.STAGING_PRODUCT (
    PRODUCT_ID,
    CATEGORY,
    SUB_CATEGORY,
    PRODUCT_NAME
)
select 
    PRODUCT_ID,
    CATEGORY,
    SUB_CATEGORY,
    PRODUCT_NAME
from SUPERSTORE.LANDING_LAYER.LANDING_PRODUCT lp 
where not exists (
    select *
    from SUPERSTORE.STAGING_LAYER.STAGING_PRODUCT sp 
    where lp.PRODUCT_ID = sp.PRODUCT_ID
    and lp.CATEGORY = sp.CATEGORY
    and lp.SUB_CATEGORY = sp.SUB_CATEGORY
    and lp.PRODUCT_NAME = sp.PRODUCT_NAME
);

-- SHIPMENT  
create or replace task INSERT_SHIPMENT_TASK 
    warehouse = compute_wh
    schedule = '1 MINUTE'
as
insert into SUPERSTORE.STAGING_LAYER.STAGING_SHIPMENT (
    SHIPMENT_ID, 
    ORDER_ID, 
    SHIP_DATE, 
    SHIP_MODE, 
    SEGMENT, 
    COUNTRY, 
    CITY, 
    STATE, 
    POSTAL_CODE, 
    REGION
)
select 
    SHIPMENT_ID, 
    ORDER_ID, 
    SHIP_DATE, 
    SHIP_MODE, 
    SEGMENT, 
    COUNTRY, 
    CITY, 
    STATE, 
    POSTAL_CODE, 
    REGION
from SUPERSTORE.LANDING_LAYER.LANDING_SHIPMENT ls
where not exists (
    select *
    from SUPERSTORE.STAGING_LAYER.STAGING_SHIPMENT ss
    where ls.SHIPMENT_ID = ss.SHIPMENT_ID 
    and ls.ORDER_ID = ss.ORDER_ID 
    and ls.SHIP_DATE = ss.SHIP_DATE 
    and ls.SHIP_MODE = ss.SHIP_MODE 
    and ls.SEGMENT = ss.SEGMENT 
    and ls.COUNTRY = ss.COUNTRY 
    and ls.CITY = ss.CITY 
    and ls.STATE = ss.STATE 
    and ls.POSTAL_CODE = ss.POSTAL_CODE 
    and ls.REGION = ss.REGION 
);
show tasks;


/* 
--------------------------------------------------------
  [MANDATORY] CREATE STREAM TO CAPTURE NEW DATA ARRIVAL
--------------------------------------------------------
*/

-- [MANDATORY] CREATE STREAMS 
-- CUSTOMER 
create or replace stream CUSTOMER_STREAM 
    on table SUPERSTORE.STAGING_LAYER.STAGING_CUSTOMER;

-- ORDER_INFOR
create or replace stream ORDER_INFOR_STREAM 
    on table SUPERSTORE.STAGING_LAYER.STAGING_ORDER_INFOR;

-- ORDER_PRODUCT
create or replace stream ORDER_PRODUCT_STREAM
    on table SUPERSTORE.STAGING_LAYER.STAGING_ORDER_PRODUCT;

-- PRODUCT
create or replace stream PRODUCT_STREAM
    on table SUPERSTORE.STAGING_LAYER.STAGING_PRODUCT;

-- SHIPMENT 
create or replace stream SHIPMENT_STREAM
    on table SUPERSTORE.STAGING_LAYER.STAGING_SHIPMENT;
----------------------------------------------------------

/* 
--------------------------------------------------------
  PROCEDURE
--------------------------------------------------------
*/
-- CUSTOMER 
CREATE OR REPLACE PROCEDURE SP_MERGE_CUSTOMER_HISTORY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    -- Create a temporary table to store data from stream
    CREATE OR REPLACE TEMPORARY TABLE TEMP_CUSTOMER_STREAM AS
    SELECT 
        CUSTOMER_ID,
        CUSTOMER_NAME,
        METADATA$ACTION AS ACTION,
        METADATA$ISUPDATE AS IS_UPDATE
    FROM CUSTOMER_STREAM;

    -- Close current record if it has the same CUSTOMER_ID 
    MERGE INTO SUPERSTORE.HISTORY_LAYER.HISTORY_CUSTOMER AS t
    USING TEMP_CUSTOMER_STREAM AS s
    ON t.CUSTOMER_ID = s.CUSTOMER_ID AND t.IS_CURRENT = TRUE
    WHEN MATCHED 
        AND s.ACTION = 'INSERT'
    THEN UPDATE SET 
        t.IS_CURRENT = FALSE,
        t.END_DATE = CURRENT_DATE;

    -- Insert new record or update record
    INSERT INTO SUPERSTORE.HISTORY_LAYER.HISTORY_CUSTOMER (
        CUSTOMER_ID,
        CUSTOMER_NAME,
        START_DATE,
        END_DATE,
        IS_CURRENT
    )
    SELECT 
        s.CUSTOMER_ID,
        s.CUSTOMER_NAME,
        CURRENT_DATE,
        NULL,
        TRUE
    FROM TEMP_CUSTOMER_STREAM s
    WHERE 
        s.ACTION = 'INSERT';

    RETURN 'SUCCESS';

END;
$$;

CREATE OR REPLACE PROCEDURE SP_MERGE_ORDER_INFOR_HISTORY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    -- Create a temporary table to store data from stream
    CREATE OR REPLACE TEMPORARY TABLE TEMP_ORDER_INFOR_STREAM AS
    SELECT 
        ORDER_ID,
        ORDER_DATE,
        CUSTOMER_ID,
        METADATA$ACTION as ACTION,
        METADATA$ISUPDATE as IS_UPDATE
    FROM ORDER_INFOR_STREAM;

    -- Close current record if it has the same ORDER_ID 
    MERGE INTO SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_INFOR AS t
    USING TEMP_ORDER_INFOR_STREAM AS s
    ON t.ORDER_ID = s.ORDER_ID AND t.IS_CURRENT = TRUE
    WHEN MATCHED 
        AND s.ACTION = 'INSERT'
    THEN UPDATE SET 
        t.IS_CURRENT = FALSE,
        t.END_DATE = CURRENT_DATE;

    -- Insert new record or update record
    INSERT INTO SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_INFOR (
        ORDER_ID,
        ORDER_DATE,
        CUSTOMER_ID,
        START_DATE,
        END_DATE,
        IS_CURRENT
    )
    SELECT 
        s.ORDER_ID,
        s.ORDER_DATE,
        s.CUSTOMER_ID,
        current_date,
        null,
        TRUE
    FROM TEMP_ORDER_INFOR_STREAM s
    WHERE 
        s.ACTION = 'INSERT';

    RETURN 'SUCCESS';

END;
$$;

CREATE OR REPLACE PROCEDURE SP_MERGE_ORDER_PRODUCT_HISTORY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    -- Create a temporary table to store data from stream
    CREATE OR REPLACE TEMPORARY TABLE TEMP_ORDER_PRODUCT_STREAM AS
    SELECT 
        ID,
        ORDER_ID,
        PRODUCT_ID,
        SALES,
        QUANTITY,
        DISCOUNT,
        PROFIT,
        METADATA$ACTION as ACTION,
        METADATA$ISUPDATE as IS_UPDATE
    FROM ORDER_PRODUCT_STREAM;

    -- Close current record if it has the same ID 
    MERGE INTO SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_PRODUCT AS t
    USING TEMP_ORDER_PRODUCT_STREAM AS s
    ON t.ID = s.ID AND t.IS_CURRENT = TRUE
    WHEN MATCHED 
        AND s.ACTION = 'INSERT'
    THEN UPDATE SET 
        t.IS_CURRENT = FALSE,
        t.END_DATE = CURRENT_DATE;

    -- Insert new record or update record
    INSERT INTO SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_PRODUCT (
        ID,
        ORDER_ID,
        PRODUCT_ID,
        SALES,
        QUANTITY,
        DISCOUNT,
        PROFIT,
        START_DATE,
        END_DATE,
        IS_CURRENT
    )
    SELECT 
        s.ID,
        s.ORDER_ID,
        s.PRODUCT_ID,
        s.SALES,
        s.QUANTITY,
        s.DISCOUNT,
        s.PROFIT,
        current_date,
        null,
        TRUE
    FROM TEMP_ORDER_PRODUCT_STREAM s
    WHERE 
        s.ACTION = 'INSERT';

    RETURN 'SUCCESS';

END;
$$;

CREATE OR REPLACE PROCEDURE SP_MERGE_PRODUCT_HISTORY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    -- Create a temporary table to store data from stream
    CREATE OR REPLACE TEMPORARY TABLE TEMP_PRODUCT_STREAM AS
    SELECT 
        PRODUCT_ID,
        CATEGORY,
        SUB_CATEGORY, 
        PRODUCT_NAME,
        METADATA$ACTION as ACTION,
        METADATA$ISUPDATE as IS_UPDATE
    FROM PRODUCT_STREAM;

    -- Close current record if it has the same PRODUCT_ID 
    MERGE INTO SUPERSTORE.HISTORY_LAYER.HISTORY_PRODUCT AS t
    USING TEMP_PRODUCT_STREAM AS s
    ON t.PRODUCT_ID = s.PRODUCT_ID AND t.IS_CURRENT = TRUE
    WHEN MATCHED 
        AND s.ACTION = 'INSERT'
    THEN UPDATE SET 
        t.IS_CURRENT = FALSE,
        t.END_DATE = CURRENT_DATE;

    -- Insert new record or update record
    INSERT INTO SUPERSTORE.HISTORY_LAYER.HISTORY_PRODUCT (
        PRODUCT_ID,
        CATEGORY,
        SUB_CATEGORY, 
        PRODUCT_NAME,
        START_DATE,
        END_DATE,
        IS_CURRENT
    )
    SELECT 
        s.PRODUCT_ID,
        s.CATEGORY,
        s.SUB_CATEGORY,
        s.PRODUCT_NAME, 
        current_date,
        null,
        TRUE 
    FROM TEMP_PRODUCT_STREAM s
    WHERE 
        s.ACTION = 'INSERT';

    RETURN 'SUCCESS';

END;
$$;



CREATE OR REPLACE PROCEDURE SP_MERGE_SHIPMENT_HISTORY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    -- Create a temporary table to store data from stream
    CREATE OR REPLACE TEMPORARY TABLE TEMP_SHIPMENT_STREAM AS
    SELECT 
        SHIPMENT_ID, 
        ORDER_ID, 
        SHIP_DATE, 
        SHIP_MODE, 
        SEGMENT, 
        COUNTRY, 
        CITY, 
        STATE, 
        POSTAL_CODE, 
        REGION, 
        METADATA$ACTION as ACTION,
        METADATA$ISUPDATE as IS_UPDATE
    FROM SHIPMENT_STREAM;

    -- Close current record if it has the same SHIPMENT_ID 
    MERGE INTO SUPERSTORE.HISTORY_LAYER.HISTORY_SHIPMENT AS t
    USING TEMP_SHIPMENT_STREAM AS s
    ON t.SHIPMENT_ID = s.SHIPMENT_ID AND t.IS_CURRENT = TRUE
    WHEN MATCHED 
        AND s.ACTION = 'INSERT'
    THEN UPDATE SET 
        t.IS_CURRENT = FALSE,
        t.END_DATE = CURRENT_DATE;

    -- Insert new record or update record
    INSERT INTO SUPERSTORE.HISTORY_LAYER.HISTORY_SHIPMENT (
        SHIPMENT_ID, 
        ORDER_ID, 
        SHIP_DATE, 
        SHIP_MODE, 
        SEGMENT, 
        COUNTRY, 
        CITY, 
        STATE, 
        POSTAL_CODE, 
        REGION, 
        START_DATE,
        END_DATE,
        IS_CURRENT
    )
    SELECT 
        s.SHIPMENT_ID, 
        s.ORDER_ID, 
        s.SHIP_DATE, 
        s.SHIP_MODE, 
        s.SEGMENT, 
        s.COUNTRY, 
        s.CITY, 
        s.STATE, 
        s.POSTAL_CODE, 
        s.REGION, 
        current_date,
        null,
        TRUE 
    FROM TEMP_SHIPMENT_STREAM s
    WHERE 
        s.ACTION = 'INSERT';

    RETURN 'SUCCESS';

END;
$$;


/* 
--------------------------------------------------------
  CREATE TASK TO MERGE DATA FROM STAGING INTO HISTORY
                 using SCD TYPE 2
--------------------------------------------------------
*/
-- create a task to merge CUSTOMER table
create or replace task MERGE_CUSTOMER_TASK 
    warehouse = compute_wh
    after SUPERSTORE.STAGING_LAYER.INSERT_CUSTOMER_TASK
as 
call SP_MERGE_CUSTOMER_HISTORY();

-- create a task to merge ORDER_INFOR table
create or replace task MERGE_ORDER_INFOR_TASK
    warehouse = compute_wh
    after SUPERSTORE.STAGING_LAYER.INSERT_ORDER_INFOR_TASK
as
call SP_MERGE_ORDER_INFOR_HISTORY();

-- create a task to merge ORDER_PRODUCT table
create or replace task MERGE_ORDER_PRODUCT_TASK 
    warehouse = compute_wh
    after SUPERSTORE.STAGING_LAYER.INSERT_ORDER_PRODUCT_TASK
as
call SP_MERGE_ORDER_PRODUCT_HISTORY();

-- create a task to merge PRODUCT table
create or replace task MERGE_PRODUCT_TASK
    warehouse = compute_wh
    after SUPERSTORE.STAGING_LAYER.INSERT_PRODUCT_TASK
as
call SP_MERGE_PRODUCT_HISTORY();

-- create a task to merge SHIPMENT table
create or replace task MERGE_SHIPMENT_TASK
    warehouse = compute_wh
    after SUPERSTORE.STAGING_LAYER.INSERT_SHIPMENT_TASK
as 
call SP_MERGE_SHIPMENT_HISTORY();

show tasks;
----------------------------------------------------------


/* 
--------------------------------------------------------
    CREATE TASK TO DELETE DATA in LANDING-LAYER
             AFTER MERGING into HISTORY
--------------------------------------------------------
*/
-- CUSTOMER table
create or replace task DELETE_CUSTOMER_TASK
    warehouse = compute_wh
    after SUPERSTORE.STAGING_LAYER.MERGE_CUSTOMER_TASK
as
delete from SUPERSTORE.LANDING_LAYER.LANDING_CUSTOMER lc
where lc.CUSTOMER_ID in (
    select hc.CUSTOMER_ID
    from SUPERSTORE.HISTORY_LAYER.HISTORY_CUSTOMER hc
);

-- ORDER_INFOR table
create or replace task DELETE_ORDER_INFOR_TASK
    warehouse = compute_wh
    after SUPERSTORE.STAGING_LAYER.MERGE_ORDER_INFOR_TASK
as 
delete from SUPERSTORE.LANDING_LAYER.LANDING_ORDER_INFOR lo
where lo.ORDER_ID in (
    select ho.ORDER_ID
    from SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_INFOR ho
);

-- ORDER_PRODUCT table
create or replace task DELETE_ORDER_PRODUCT_TASK
    warehouse = compute_wh
    after SUPERSTORE.STAGING_LAYER.MERGE_ORDER_PRODUCT_TASK
as 
delete from SUPERSTORE.LANDING_LAYER.LANDING_ORDER_PRODUCT lo
where lo.ID in (
    select ho.ID
    from SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_PRODUCT ho
);

-- PRODUCT table
create or replace task DELETE_PRODUCT_TASK
    warehouse = compute_wh
    after SUPERSTORE.STAGING_LAYER.MERGE_PRODUCT_TASK
as 
delete from SUPERSTORE.LANDING_LAYER.LANDING_PRODUCT lp
where lp.PRODUCT_ID in (
    select hp.PRODUCT_ID
    from SUPERSTORE.HISTORY_LAYER.HISTORY_PRODUCT hp
);

-- SHIPMENT table
create or replace task DELETE_SHIPMENT_TASK
    warehouse = compute_wh
    after SUPERSTORE.STAGING_LAYER.MERGE_SHIPMENT_TASK
as 
delete from SUPERSTORE.LANDING_LAYER.LANDING_SHIPMENT ls
where ls.SHIPMENT_ID in (
    select hs.SHIPMENT_ID
    from SUPERSTORE.HISTORY_LAYER.HISTORY_SHIPMENT hs
);

show tasks;