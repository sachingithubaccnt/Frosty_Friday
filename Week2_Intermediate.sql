Use database Frosty_Friday_Challenges;

create schema Week_2_Intermediate;

create or replace file format my_parquet_format type = parquet;

create or replace stage challenge_2_stage url='s3://frostyfridaychallenges/challenge_2/'
file_format = Frosty_Friday_Challenges.Week_2_Intermediate.my_parquet_format;

list @challenge_2_stage

--- Get the Column name and their respective Data Type
select generate_column_description(array_agg(object_construct(*)), 'table') as columns
  from table (
    infer_schema(
      location=>'@challenge_2_stage/',
      file_format=>'my_parquet_format'
    )
  );

--Create the table automatically using Infer Schema

create or replace table PARQUET_TABLE using template
(select array_agg(object_construct(*)) from table 
 (
    infer_schema(
      location=>'@challenge_2_stage/',
      file_format=>'my_parquet_format'
    )
   ))
   
--Load Data into Parque table

 copy into PARQUET_TABLE 
  from 
( select 
 
 $1:email, $1:country, $1:country_code, $1:education, $1:postcode, $1:first_name, $1:street_name, $1:job_title, $1:city,
 $1:employee_id,$1:last_name,$1:time_zone,$1:street_num,$1:payroll_iban, $1:suffix,$1:dept, $1:title
from @challenge_2_stage/)


select * from PARQUET_TABLE

create or replace view PARQUET_VIEW
as
select
     "employee_id","dept","job_title" from PARQUET_TABLE ;


select * from PARQUET_VIEW

create stream PARQUET_VIEW_STREAM on view PARQUET_VIEW;

SELECT * FROM PARQUET_VIEW_STREAM


-- execute the following commands
update PARQUET_TABLE set "country" = 'Japan' where "employee_id" = 8;
update PARQUET_TABLE set "last_name" = 'Forester' where "employee_id" = 22;
update PARQUET_TABLE set "dept" = 'Marketing' where "employee_id" = 25;
update PARQUET_TABLE set "title" = 'Ms' where "employee_id" = 32;
update PARQUET_TABLE set "job_title" = 'Senior Financial Analyst' where "employee_id" = 68;

-- Check result
select * from   PARQUET_VIEW_STREAM;
