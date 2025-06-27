/* 
--------------------------------------------------------
                    HISTORY-LAYER
            merged data from STAGING_LAYER
--------------------------------------------------------
*/

-- choose database and schemas
use database SUPERSTORE;
use schema history_layer;
show schemas;

-- create tables
create or replace table history_customer (
    CUSTOMER_ID varchar(20),
    CUSTOMER_NAME varchar(50),
    START_DATE date,
    END_DATE date,
    IS_CURRENT boolean
);

create or replace table history_order_infor (
    ORDER_ID varchar(20),
    ORDER_DATE date,
    CUSTOMER_ID varchar(20),
    START_DATE date,
    END_DATE date,
    IS_CURRENT boolean
);

create or replace table history_order_product (
    ID number(38,0),
    ORDER_ID varchar(20),
    PRODUCT_ID varchar(20),
    SALES number(38, 4),
    QUANTITY number(38, 0),
    DISCOUNT number(38, 2),
    PROFIT number(38,4),
    START_DATE date,
    END_DATE date,
    IS_CURRENT boolean
);

create or replace table history_product (
    PRODUCT_ID varchar(20),
    CATEGORY varchar(50),
    SUB_CATEGORY varchar(50),
    PRODUCT_NAME varchar(200),
    START_DATE date,
    END_DATE date,
    IS_CURRENT boolean
);

create or replace  table history_shipment (
    SHIPMENT_ID NUMBER(38,0), 
    ORDER_ID VARCHAR(16777216), 
    SHIP_DATE DATE,
    SHIP_MODE VARCHAR(16777216), 
    SEGMENT VARCHAR(16777216), 
    COUNTRY VARCHAR(16777216), 
    CITY VARCHAR(16777216), 
    STATE VARCHAR(16777216), 
    POSTAL_CODE NUMBER(38,0), 
    REGION VARCHAR(16777216),
    START_DATE date,
    END_DATE date,
    IS_CURRENT boolean
);

show tables;
    





