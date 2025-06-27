/* 
--------------------------------------------------------
                    Semantic views
                 joining DIM and FACT
--------------------------------------------------------
*/
use database SUPERSTORE;
use schema semantic_layer;

/* 
--------------------------------------------------------
            Create a view of each order product
--------------------------------------------------------
*/
create or replace view VIEW_FACT_ORDER_PRODUCT as
select 
    f.ORDER_PRODUCT_ID,
    f.ORDER_ID,
    f.SALES,
    f.QUANTITY,
    f.DISCOUNT,
    f.PROFIT,
    
    c.CUSTOMER_ID,
    c.CUSTOMER_NAME,

    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY as PRODUCT_CATEGORY,
    p.SUB_CATEGORY as PRODUCT_SUB_CATEGORY,
    
    o.ORDER_DATE,
    day(o.ORDER_DATE) as ORDER_DAY,
    month(o.ORDER_DATE) as ORDER_MONTH,
    year(o.ORDER_DATE) as ORDER_YEAR,

    s.SHIP_DATE,
    s.SHIP_MODE,
    s.SEGMENT,
    s.CITY,
    s.STATE,
    s.COUNTRY,
    s.REGION    
    
from SUPERSTORE.DIM_AND_FACT.FACT_ORDER_PRODUCT f
left join SUPERSTORE.DIM_AND_FACT.DIM_CUSTOMER c on f.CUSTOMER_ID = c.CUSTOMER_ID
left join SUPERSTORE.DIM_AND_FACT.DIM_ORDER o on f.ORDER_ID = o.ORDER_ID 
left join SUPERSTORE.DIM_AND_FACT.DIM_PRODUCT p on f.PRODUCT_ID = p.PRODUCT_ID 
left join SUPERSTORE.DIM_AND_FACT.DIM_SHIPMENT s on f.ORDER_ID = s.ORDER_ID;

select * from VIEW_FACT_ORDER_PRODUCT;