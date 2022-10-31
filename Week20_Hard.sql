CREATE OR REPLACE PROCEDURE clone_privilges (src_db  VARCHAR ,src_schema VARCHAR,target_db varchar,target_schema varchar)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var counter = 1;
var privname = "";
var granteename = "";
var sql_text = "SHOW GRANTS ON SCHEMA " + SRC_DB + "." + SRC_SCHEMA;
var temp_tbl = `create TEMPORARY table DEMO_DB.PUBLIC.GRANTS_PRIV AS SELECT "privilege","grantee_name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) where "grantee_name" <> 'ACCOUNTADMIN'`;
var CLONE_QUERY = "CREATE SCHEMA IF NOT EXISTS " + TARGET_DB  + "." + TARGET_SCHEMA  + " CLONE " + SRC_DB + "." + SRC_SCHEMA ;
//Create the CLONE SCHEMA accepting input as an argument
try
{
CLONE_STMT = snowflake.createStatement({sqlText:CLONE_QUERY});
CLONE_EXECUTE = CLONE_STMT.execute();
CLONE_EXECUTE.next();
CLONE_STATUS = CLONE_EXECUTE.getColumnValue(1);
if(CLONE_STATUS.includes("successfully"))
{
var st = snowflake.createStatement({ sqlText: sql_text}); //Show the Grants on source DB
var resultSet = st.execute();
var tname = snowflake.createStatement({ sqlText: temp_tbl}); //Create Temp table to store all the grants
var rs = tname.execute();
sql_cmd = `select "privilege","grantee_name" from DEMO_DB.PUBLIC.GRANTS_PRIV order by "grantee_name"`; //Select the privilges needs to be grant to the clone schema
var sql_stmt = snowflake.createStatement({sqlText: sql_cmd});
var rs=sql_stmt.execute();
var LastRow = rs.getRowCount();
while (counter <= LastRow)
{
rs.next();
privname = rs.getColumnValue(1);
granteename = rs.getColumnValue(2);
var sql_grant = `GRANT ` + privname + ` ON SCHEMA ` + TARGET_DB + "." + TARGET_SCHEMA + ` TO ROLE ` + granteename ; //Grant the privilges
try {
var stmt = snowflake.createStatement({ sqlText: sql_grant});
var result = stmt.execute();
}
catch (err) 
{ 
return "Failed: " + err;
}
 counter += 1;
}
}

}
catch(err)
{
            return "Exception Occurred: " + err;
};
        
return "out";

$$;
