
use database SUPERSTORE;
/* 
--------------------------------------------------------
                    CREATE SCHEMAS 
    for landing-layer, staging-layer, history-layer
--------------------------------------------------------
*/
create or replace schema landing_layer;
create or replace schema staging_layer;
create or replace schema history_layer;
create or replace schema dim_and_fact;
create or replace schema semantic_layer;
show schemas;

