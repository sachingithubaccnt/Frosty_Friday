use database frosty_friday;

create schema Week_4_Basic;

create file format my_json_format
type = json
strip_outer_array = true
;

-- Create stage 
create or replace stage challenge_4_stage
  url = 's3://frostyfridaychallenges/challenge_4/'
  file_format = Frosty_Friday_Challenges.Week_4_Basic.my_json_format;
;

-- Check stage contents
list @challenge_4_stage
;

select t.metadata$filename
,      t.metadata$file_row_number
,      t.$1
from   @challenge_4_stage t
;

-- Create table with parsed JSON
create table WEEK_4_JSON (SRC variant)
;

copy into WEEK_4_JSON
from
(select t.$1 from   @challenge_4_stage t
)
;


SELECT * FROM WEEK_4_JSON;

create table WEEK_4_PARSE_JSON as
SELECT 
row_number() OVER (ORDER BY M.value:"Birth"::date) as ID,
row_number() over(ORDER BY 1)  AS INTER_HOUSE_ID,
SRC:"Era"::varchar as ERA,
H.VALUE:"House" ::string as House,
M.value:Name::varchar as name,
M.value:Nickname[0]::varchar as NickName1,
M.value:Nickname[1]::varchar as NickName2,
M.value:Nickname[2]::varchar as NickName3,
M.value:Birth::date as birth,
M.value:"Place of Birth"::Varchar as Birth_Place,
M.value:"Place of Death"::varchar as Death_Place,
M.value:"Start of Reign"::date as Start_of_Reign,
M.value:"End of Reign"::date as End_of_Reign,
M.value:"Duration"::varchar as duration,
M.value:"Death"::date as death,
M.value:"Consort\/Queen Consort"[0]::varchar as QUEEN_CONSORT_1,
M.value:"Consort\/Queen Consort"[1]::varchar as QUEEN_CONSORT_2,
M.value:"Consort\/Queen Consort"[2]::varchar as QUEEN_CONSORT_3,
M.value:"Age at Time of Death"::varchar as AGE_AT_DEATH,
M.value:"Burial Place"::varchar as Burried_Place
FROM 
WEEK_4_JSON WK,
LATERAL FLATTEN(SRC:"Houses") as H,
LATERAL FLATTEN(H.value:"Monarchs") as M
;

select * from WEEK_4_PARSE_JSON
