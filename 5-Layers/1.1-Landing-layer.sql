/* 
--------------------------------------------------------
                    LANDING-LAYER
            create tables of landing-layer
--------------------------------------------------------
*/

-- SELECT DATABASE 
use database SUPERSTORE;


-- SELECT SCHEMA
use schema landing_layer;


/* 
--------------------------------------------------------
                 CREATE STRANSIENT TABLES 
customer, order_infor, order_product, product, shipment
--------------------------------------------------------
*/
-- create customer table
create or replace transient table landing_customer (
    CUSTOMER_ID varchar,
    CUSTOMER_NAME varchar
);

--create order_infor table
create or replace transient table landing_order_infor (
    ORDER_ID varchar,
    ORDER_DATE date,
    CUSTOMER_ID varchar
);

-- create order_product table 
create or replace transient table landing_order_product (
    ID NUMBER(38,0), 
    ORDER_ID VARCHAR(16777216),
    PRODUCT_ID VARCHAR(16777216),
    SALES NUMBER(38,4),
    QUANTITY NUMBER(38,0),
    DISCOUNT NUMBER(38,2),
    PROFIT NUMBER(38,4)
);

-- create product table
create or replace transient table landing_product (
    PRODUCT_ID VARCHAR(),
    CATEGORY VARCHAR(),
    SUB_CATEGORY VARCHAR(),
    PRODUCT_NAME VARCHAR()
);

--create shipment table
create or replace transient table landing_shipment (
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
                CREATE FILE FORMAT (CSV) 
--------------------------------------------------------
*/
create or replace FILE FORMAT csv_file_format
    compression = auto
    type = 'csv'
    skip_header = 1
    field_delimiter = ','
    record_delimiter = '\n'
    field_optionally_enclosed_by = '"';

    
/* 
--------------------------------------------------------
            UPLOAD FILE USING FILE FORMART 
        has just been defined `csv_file_format`
                doing by using UI
--------------------------------------------------------
*/    



-- query tables to see the data
select * from SUPERSTORE.LANDING_LAYER.LANDING_CUSTOMER limit 5;
select * from SUPERSTORE.LANDING_LAYER.LANDING_ORDER_INFOR limit 5;
select * from SUPERSTORE.LANDING_LAYER.LANDING_ORDER_PRODUCT limit 5;
select * from SUPERSTORE.LANDING_LAYER.LANDING_PRODUCT limit 5;


/* 
--------------------------------------------------------
            [FOR TESTING] MODIFY DATA in TABLE 
--------------------------------------------------------
*/
insert into SUPERSTORE.LANDING_LAYER.LANDING_CUSTOMER (
    CUSTOMER_ID,
    CUSTOMER_NAME
) values (
    'CG-12520',
    'Tuyet'
);

-- insert into SUPERSTORE.LANDING_LAYER.LANDING_CUSTOMER (
--     CUSTOMER_ID,
--     CUSTOMER_NAME
-- ) values (
--     'CG-12520',
--     'Anh Tuyet'
-- );

-- -- delete new rows
-- delete from SUPERSTORE.LANDING_LAYER.LANDING_CUSTOMER
-- where CUSTOMER_ID = '001' or CUSTOMER_ID = '002'; 

-- select * from SUPERSTORE.LANDING_LAYER.LANDING_CUSTOMER;
