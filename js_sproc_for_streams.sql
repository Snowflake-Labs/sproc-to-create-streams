/*************************************************************************************************************
Script:             Enable streams on tables & views in inbound shares
Create Date:        2023-04-10
Author:             Gopal Raghavan
Description:        Stored Procedure to create streams on tables & views in inbound shares


Copyright © 2023 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-04-20          G. Raghavan                         Initial Creation
*************************************************************************************************************/

//set the roles and the warehouse
use role accountadmin;
//use whatever warehouse. Xsmall is ok
//the warehouse needs to be created as a prerequisite
use warehouse osr;

//create the database to hold all the audit objects
create database consumer_snowflake_sproc;
create schema consumer_sproc;

//create temporary tables to hold all the objects for audit purposes
//if need be the word TEMPORARY can be removed to create permanent tables
CREATE OR REPLACE TEMPORARY TABLE SHARE_DESC
(DESC_STMT VARCHAR)
COMMENT = 'CONTAINS DESCRIBE STATEMENTS FOR EVERY SHARE';

CREATE OR REPLACE TEMPORARY TABLE SHARE_AND_TYPE
(SHARE_OBJ_TYPE VARCHAR,
SHARE_OBJ_NAME VARCHAR)
COMMENT = 'CONTAINS THE TYPE OF OBJECT OF EACH INBOUND SHARE';

CREATE OR REPLACE TEMPORARY TABLE STREAMS_TRACKING_AUDIT
(CHANGE_ACTION VARCHAR)
COMMENT = 'CONTAINS THE LOG OF ALL CREATE STREAM ACTIONS';


//***********************************************************************
//THIS BLOCK CREATES THE STORED PROCEDURE
//***********************************************************************
create or replace procedure enable_streams()
  returns string not null
  language javascript
  execute as caller
  as     
  $$ 

    function create_streams(typestr,tblview) {
        
        //remove the database and schema name from the share name
        //since the actual table is the last in the array, we'll use
        //pop() to pick it up
        
        tblview2 = tblview.split(".").pop();

        //for names that are in double quotes, we need to find the last instance
        //of the " and append _STREAM" to it

        if (tblview2.includes('"')){
            const lastIndex = tblview2.lastIndexOf('"');
            var modstmt = tblview2.slice(0, lastIndex) +'_STREAM"';
        }
        else {
            var modstmt = tblview2+ '_STREAM';
        }
        
        //execute the CREATE STREAM statement
        
        if (typestr == "TABLE") {
            stmt = 'CREATE STREAM IF NOT EXISTS ' +modstmt+ ' ON TABLE ' +tblview;
        }
        else {
            stmt = 'CREATE STREAM IF NOT EXISTS ' +modstmt+ ' ON VIEW ' +tblview;
        }
        
        sql_stmt = snowflake.createStatement({sqlText: stmt});
        try {
                sql_cmd = sql_stmt.execute();
                var alter_success_stmt = 'INSERT INTO STREAMS_TRACKING_AUDIT (CHANGE_ACTION) VALUES (\'' +tblview+ ' HAS STREAMS ENABLED\')';
                var alter_success_cmd = snowflake.createStatement({sqlText: alter_success_stmt});
                var alter_success = alter_success_cmd.execute();
            }
        catch(err){
            //grab all the error information
            var result =  "Failed: Code: " + err.code + "  State: " + err.state;
            //remove the single quote from err.message object to prevent SQL errors
            //when inserting into the STREAMS_TRACKING_AUDIT table
            const msg = err.message.replace(/\'/g, "");
            result += "  Message: " +msg;
            result += " Stack Trace: " + err.stackTraceTxt;
            
            var alter_err_stmt = 'INSERT INTO STREAMS_TRACKING_AUDIT (CHANGE_ACTION) VALUES (\'' +tblview+ ' encountered error: ' + result+ '\')';
            var alter_err_cmd = snowflake.createStatement({sqlText: alter_err_stmt});
            var alter_err = alter_err_cmd.execute();
        }
        return "streams was attempted to be enabled on table or view";
    }
    
    var my_sql_command = "show shares";
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();

    //we want to exclude any Snowflake administrative or sample data
    
    var first_sql_cmd = 'SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) where "kind" = \'INBOUND\' and "database_name" NOT IN (\'SNOWFLAKE\', \'SNOWFLAKE_SAMPLE_DATA\')';
    var first_sql_stmt = snowflake.createStatement({sqlText: first_sql_cmd});
    var res_first_sql = first_sql_stmt.execute();
    
    //add all the shares with a DESCRIBE COMMAND into the DESC table
    
    while (res_first_sql.next())  {
       
       //Read share name
       var share = res_first_sql.getColumnValue(1);
       var share_str = '(\'DESCRIBE SHARE ' +share+ ' \')';
       var desc_share_cmd = 'INSERT INTO SHARE_DESC (DESC_STMT) VALUES ' +share_str;
       var prep_stmt = snowflake.createStatement({sqlText: desc_share_cmd});
       var exec_stmt = prep_stmt.execute();
       
       }
       
    //loop through the DESC table and execute the statement
    //and pick up the name and kind from the RESULT_SCAN function

    var desc_stmt = 'SELECT * FROM SHARE_DESC';
    var desc_cmd = snowflake.createStatement({sqlText: desc_stmt});
    var desc_exec = desc_cmd.execute();

    //execute each statment in a loop

    while (desc_exec.next()){

        var desc_share_name = desc_exec.getColumnValue(1);
        var prep_desc_stmt = snowflake.createStatement({sqlText: desc_share_name});
        var prep_desc_cmd = prep_desc_stmt.execute();

        while (prep_desc_cmd.next()) {
            var pickup_view_table_stmt = 'SELECT "kind", "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) where "kind" IN (\'TABLE\', \'VIEW\')';
            pickup_view_table_cmd = snowflake.createStatement({sqlText: pickup_view_table_stmt});
             res_pickup_view_table = pickup_view_table_cmd.execute();

             //Add the components on each share into SHARE_AND_TYPE table

             while (res_pickup_view_table.next()) {

                var type_str = res_pickup_view_table.getColumnValue(1);
                var name_str = res_pickup_view_table.getColumnValue(2);
                var pickup_view_tbl_stmt = 'INSERT INTO SHARE_AND_TYPE (SHARE_OBJ_TYPE, SHARE_OBJ_NAME) VALUES (\'' +type_str+ '\', \'' +name_str+ '\')';
                var pickup_view_tbl_cmd = snowflake.createStatement({sqlText: pickup_view_tbl_stmt});
                var pickup_view_tbl_exec = pickup_view_tbl_cmd.execute();

             }

          break;
            
        }
        
    }

    //Now that the table SHARE_AND_TYPE has been populated,
    //Enable streams for all tables;

    var streams_tbl_stmt = 'SELECT SHARE_OBJ_NAME FROM SHARE_AND_TYPE WHERE SHARE_OBJ_TYPE = \'TABLE\'';
    var streams_tbl_cmd = snowflake.createStatement({sqlText: streams_tbl_stmt});
    var streams_tbl_exec = streams_tbl_cmd.execute();

    while (streams_tbl_exec.next()) {

        //loop through the tables
        var tbl = streams_tbl_exec.getColumnValue(1);
        //call function to enable streams
        var typestr = 'TABLE';
        var streams_tbl_true_exec = create_streams(typestr, tbl);
    }

    //The second step is to pick up all the views and then create streams on them

    var streams_view_stmt = 'SELECT SHARE_OBJ_NAME FROM SHARE_AND_TYPE WHERE SHARE_OBJ_TYPE = \'VIEW\'';
    var streams_view_cmd = snowflake.createStatement({sqlText: streams_view_stmt});
    var streams_view_exec = streams_view_cmd.execute();

    while (streams_view_exec.next()) {

        //loop through the views
        var vw = streams_view_exec.getColumnValue(1);
        //call function to enable streams
        var typestr = 'VIEW';
        var streams_view_true_exec = create_streams(typestr, vw);
    
    }
    
  return "success"; // Replace with something more useful.
  $$
  ;

call enable_streams();

//uncomment for audit

//SELECT * FROM SHARE_DESC;
//SELECT * FROM SHARE_AND_TYPE;
//SELECT * FROM STREAMS_TRACKING_AUDIT;
