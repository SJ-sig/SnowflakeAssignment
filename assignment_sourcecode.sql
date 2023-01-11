/* ------------------------- STEP 1 ------------------------- */

-- Create a role "ADMIN" and Grant the role to "ACCOUNTADMIN"
create or replace ADMIN;
grant role ADMIN to role ACCOUNTADMIN;

-- Create a role "DEVELOPER" and Grant the role to "ADMIN"
create or replace role DEVELOPER;
grant role DEVELOPER to role ADMIN;

-- Create a role "PII" and Grant the role to "ACCOUNTADMIN"
create or replace PII;
grant role PII to role ACCOUNTADMIN;


/* ------------------------- STEP 2 ------------------------- */

-- Create a M-sized warehouse named assignment_wh

create or replace warehouse assignment_wh with
warehouse_size = MEDIUM
auto_suspend = 180
auto_resume = true
initially_suspended=true;


/* ------------------------- STEP 3 ------------------------- */

-- Switch to the ADMIN role.

grant create database on account to role ADMIN;
grant usage on warehouse assignment_wh to role admin;
show grants to role ADMIN;

use role ADMIN;

/* ------------------------- STEP 4 ------------------------- */

-- Create a database 'assignment_db'

create or replace database assignment_db;

/* ------------------------- STEP 5 ------------------------- */

-- Create a schema 'my_schema'

create or replace schema my_schema;


/* ------------------------- STEP 6 ------------------------- */

-- Create a table using any sample csv.

    // creating a transient table.

    -- Table for internal stage
        create or replace TRANSIENT table people_internal (
            s_num numeric,
            user_id text,
            first_name string,
            last_name string,
            sex varchar(10),
            email string,
            phone string,
            dob date,
            job_title string,
            elt_filename string,
            elt_ts_timestamp timestamp,
            elt_by_app string
        );
        
    -- Table for external stage
        create or replace table people_external like people_internal;


select * from people_internal;
select * from people_external;


/* ------------------------- STEP 8 ------------------------- */


-- Load the file into an internal stage.
    
    // NOTE: we are using the internal table stage to load data into snowflake as we are only loading data into a single table and we don't need to share files with multiple users.
    
    -- Use snowsql CLI for loading data into internal table staging
    
    /* put file:///Users/sahil/Downloads/people-1000000.csv 
         @assignment_db.my_schema.%people_internal;*/
                
-- Load data into external staging area.
    
    // Note: Using AWS S3 as external stage.
    
    -- Grant privileges
        
        grant create integration on account to role ADMIN;

    -- create an intergration object 
    
        create or replace storage integration integrate_s3
        type = external_stage
        storage_provider = s3
        enabled = true
        storage_aws_role_arn = 'arn:aws:iam::969703917248:role/snowflake_role'
        storage_allowed_locations = ('s3://snowflakeassignment0901/input_files/');

        desc integration integrate_s3;
        
        
    -- Create a file format option

        create or replace file format assignment_db.my_schema.my_csv_format
            type = csv 
            field_optionally_enclosed_by='"' 
            record_delimiter = '\n' 
            field_delimiter = ',' 
            skip_header = 1 
            null_if = ('NULL', 'null') 
            empty_field_as_null = true;
            -- error_on_column_count_mismatch=false;

        desc file format assignment_db.my_schema.my_csv_format;
        
    -- Create an external stage for s3 bucket
        
        create or replace stage s3_stage
        storage_integration = integrate_s3
        url = 's3://snowflakeassignment0901/input_files/'
        file_format = assignment_db.my_schema.my_csv_format;
        
        list @s3_stage;
    
    
/* ------------------------- STEP 9 ------------------------- */
    
-- load data into snowflake table using COPY command on TABLE STAGING
  
    // SCENARIO-1: load data from a large single csv file.
                
        list @assignment_db.my_schema.%people_internal;
        
        copy into assignment_db.my_schema.people_internal
        from (select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9,metadata$filename,current_timestamp(),'LOCAL' from @assignment_db.my_schema.%people_internal t)
        file_format = my_csv_format
        on_error = 'CONTINUE';

        select * from people_internal limit 10;
        

    // SCENARIO-2: load data from a multiple smaller csv files.
    
        list @assignment_db.my_schema.%people_internal;
        
        copy into assignment_db.my_schema.people_internal
        from (select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9,metadata$filename,current_timestamp(),'LOCAL' from @assignment_db.my_schema.%people_internal t)
        file_format = my_csv_format
        on_error = 'CONTINUE';

        select * from people_internal limit 10;
        
        
-- load data into snowflake table using COPY command on EXTERNAL STAGING area AWS S3

        list @assignment_db.my_schema.s3_stage;
        
        copy into assignment_db.my_schema.people_external
        from (select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9,metadata$filename,current_timestamp(),'AWS_S3' from @assignment_db.my_schema.s3_stage t)
        file_format = my_csv_format
        on_error = 'CONTINUE';

        select * from people_external limit 10;

/* ------------------------- STEP 7 ------------------------- */

-- Note: this is performed after the data load to create a variant dataset.

create or replace TRANSIENT table variant_data (
    data variant
);


insert into variant_data 
    (select to_variant(object_construct(*)) as data from people_internal limit 10);

select * from variant_data;

/* ------------------------- STEP 10 & 11 ------------------------- */

create or replace file format my_parquet_format
  type = 'parquet';
  
create or replace stage parquet_stage
file_format = my_parquet_format;

show stages;
list @parquet_stage;

select * from table(infer_schema(location=> '@parquet_stage', file_format=>'my_parquet_format'));

select $1::varchar data from @parquet_stage/userdata.parquet;


/* ------------------------- STEP 12 ------------------------- */

-- Create a MASKING policy

    create or replace masking policy assignment_db.my_schema.pii_masked as (val string) returns string ->
        case 
            when current_role() in ('PII', 'ADMIN') then val
            else '**masked**'
        end;
        
-- Attach the MASKING policy

    alter table assignment_db.my_schema.people_internal modify 
        column email set masking policy assignment_db.my_schema.pii_masked,
        column phone set masking policy assignment_db.my_schema.pii_masked;
        
  -- NOTE: Currently, Snowflake does not support different input and output data types in a masking policy, such as                   defining the masking policy to target a timestamp and return a string (e.g. ***MASKED***); the input and                 output data types must match.
  
  
-- check masking for DEVELOPER role

    -- grant necessary privileges to DEVELOPER role

    grant usage on database assignment_db to role DEVELOPER;
    grant usage on schema my_schema to role DEVELOPER;
    grant select on table assignment_db.my_schema.people_internal to role DEVELOPER;
    
    use role DEVELOPER;
     
    select * from people_internal limit 10;

    -- grant usage on warehouse assignment_wh to role developer;

-- check masking for PII role

    -- grant necessary privileges to PII role

    grant usage on database assignment_db to role PII;
    grant usage on schema my_schema to role PII;
    grant select on table assignment_db.my_schema.people_internal to role PII;
    
    use role PII;
     
    select * from people_internal limit 10;

  