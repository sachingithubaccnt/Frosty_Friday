--Create new DB and Schema for Frosty Friday assignment

create database Frosty_Friday_Challenges;
create schema Week_1_Basic;
 
--Create SV file format

 create or replace file format Frosty_Friday_Challenges.Week_1_Basic.csv_format
                    type = csv
                    skip_header = 1
                    null_if = ('NULL', 'null')
                    empty_field_as_null = true;


-- Create External stage pointing to S3 and tied the fileformat

create or replace stage Frosty_Friday_Challenges.Week_1_Basic.ext_csv_stage
  URL = 's3://frostyfridaychallenges/challenge_1/'
  file_format = Frosty_Friday_Challenges.Week_1_Basic.csv_format;
  
  
list @ext_csv_stage;

select METADATA$FILENAME::STRING as FILE_NAME,s.$1 from @ext_csv_stage s;

--Create table to store Filename and its contents

create table Week1_Data(filename varchar,file_data varchar);

copy into Week1_Data from
(select METADATA$FILENAME::STRING as FILE_NAME,s.$1 from @ext_csv_stage s);

select * from Week1_Data;
