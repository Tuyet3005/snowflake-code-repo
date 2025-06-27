/* 
--------------------------------------------------------
            Create DIM and FACT tables
--------------------------------------------------------
*/
use database SUPERSTORE;
use schema dim_and_fact;


/* 
--------------------------------------------------------
            Create DIM tables
--------------------------------------------------------
*/
-- DIM_CUSTOMER - Customer information
create or replace table DIM_CUSTOMER as
select
    CUSTOMER_ID,
    CUSTOMER_NAME
from SUPERSTORE.HISTORY_LAYER.HISTORY_CUSTOMER
where IS_CURRENT = TRUE;

select * from  SUPERSTORE.HISTORY_LAYER.HISTORY_CUSTOMER;

-- DIM_ORDER - Order information 
create or replace table DIM_ORDER as
select  
    ORDER_ID,
    ORDER_DATE,
    CUSTOMER_ID
from SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_INFOR
where IS_CURRENT = TRUE;

-- DIM_PRODUCT - Product information 
create or replace table DIM_PRODUCT as
select  
    PRODUCT_ID,
    CATEGORY,
    SUB_CATEGORY,
    PRODUCT_NAME
from SUPERSTORE.HISTORY_LAYER.HISTORY_PRODUCT
where IS_CURRENT = TRUE;

-- DIM_SHIPMENT - shipment information 
create or replace table DIM_SHIPMENT as
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
from SUPERSTORE.HISTORY_LAYER.HISTORY_SHIPMENT
where IS_CURRENT = TRUE;

/* 
--------------------------------------------------------
            Create FACT table
FACT_ORDER_PRODUCT - Containing sales, quantity, customer id, 
order id, customer id,... for each product being sold
--------------------------------------------------------
*/
create or replace table FACT_ORDER_PRODUCT as
select  
    p.ID as ORDER_PRODUCT_ID,
    p.ORDER_ID,
    o.CUSTOMER_ID,
    p.PRODUCT_ID,
    p.SALES,
    p.QUANTITY,
    p.DISCOUNT,
    p.PROFIT,
    o.ORDER_DATE,
    s.SHIP_DATE,
    s.SHIP_MODE,
    s.REGION
from SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_PRODUCT p
left join SUPERSTORE.DIM_AND_FACT.DIM_ORDER o
    on p.ORDER_ID = o.ORDER_ID
left join SUPERSTORE.DIM_AND_FACT.DIM_SHIPMENT s
    on p.ORDER_ID = s.ORDER_ID
where p.IS_CURRENT = TRUE;



/* 
--------------------------------------------------------
            Results DIM and FACT tables
--------------------------------------------------------
*/
select * from DIM_CUSTOMER;
select * from DIM_ORDER;
select * from DIM_PRODUCT;
select * from DIM_SHIPMENT;

-- compare number of rows
select count(*) from SUPERSTORE.HISTORY_LAYER.HISTORY_ORDER_PRODUCT;
select count(*) from FACT_ORDER_PRODUCT;

select * from FACT_ORDER_PRODUCT;



