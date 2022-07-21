
Use database Frosty_Friday_Challenges;
create schema Week_3_Basic;

create or replace file format my_csv_format 
    type = csv
    record_delimiter = '\n'
    skip_header = 1

create or replace stage challenge_3_stage url='s3://frostyfridaychallenges/challenge_3/'
file_format = Frosty_Friday_Challenges.Week_3_Basic.my_csv_format;

list @challenge_3_stage

CREATE OR REPLACE table WEEK3_TABLE
(file_name varchar,record_id varchar,id VARCHAR,first_name VARCHAR,last_name VARCHAR,catch_phrase VARCHAR,record_timestamp timestamp);

COPY INTO WEEK3_TABLE
FROM (
  select 
      METADATA$FILENAME::STRING as FILE_NAME
    , METADATA$FILE_ROW_NUMBER as ROW_NUMBER
    , t.$1,t.$2,t.$3,t.$4,t.$5
  from @challenge_3_stage/ (pattern=>'.*week3_.*[.]csv') t
);

create or replace table WEEK3_TABLE_KEYWORD (
    file_name varchar
  , record_id varchar
  , Keyword varchar
  , Add_By varchar
  , Non_Sense varchar
)
;

-- Ingest the data
COPY INTO WEEK3_TABLE_KEYWORD
FROM (
  select 
      METADATA$FILENAME::STRING as FILE_NAME
    , METADATA$FILE_ROW_NUMBER as ROW_NUMBER
    , t.$1,t.$2,t.$3
  from @challenge_3_stage/keywords t
)
;

select * from WEEK3_TABLE_KEYWORD

select * from WEEK3_TABLE

select file_name,count(*) from WEEK3_TABLE where file_name like any  
(select concat('%', keyword , '%') from   WEEK3_TABLE_KEYWORD)
group by file_name;

