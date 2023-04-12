# Javascript stored procedure to enable streams on tables & views in all Inbound Shares
This stored procedure will enable streams on tables and views contained in Inbound Shares.  *This will be run on the Consumer side to track any changes in the data coming from the Provider side.*

Copyright &copy; 2023 Snowflake Inc. All rights reserved.

---
>This code is not part of the Snowflake Service and is governed by the terms in LICENSE.txt, unless expressly agreed to in writing.  Your use of this code is at your own risk, and Snowflake has no obligation to support your use of this code.
---

PRIOR TO ENABLING STREAMS ON INBOUND SHARE OBJECTS
---
Please ask your Provider to turn on Change Tracking for all views and tables contained in their Outbound Shares (shared with you).  **Without this step, you will not be able to turn on streams on these objects on your end.**  They can use this [stored procedure](https://github.com/Snowflake-Labs/sproc-to-enable-change-tracking "javascript stored procedure to enable change tracking") to turn on Change Tracking. 

NOTE
---

1.  The warehouse needs to be created before executing the commands in this file.  The warehouse can be x-small.
2.  The database and schema names for the temporary tables and the stored procedure can be changed as needed.
3.  The tables to store actions for audit purposes have been defined as TEMPORARY.  These can be made permanent tables if need be.  Temporary tables will automatically be dropped if the session is expired or expires.
4.  The name of the stored procedure can be changed to suit the user's naming standards.
5.  This assumes that the role ACCOUNTADMIN or any other (with which you may be executing these statements) have the privilege to perform all the actions to enable change tracking.  These include DESCRIBE, SELECT, INSERT and ALTER actions on objects referenced in the stored procedure. 
6.  The javascript variables can be undefined and deleted to optimize memory.  I'll be doing that over time as well.
7.  To look at all the running statements in the procedure, you can use the role of an ACCOUNTADMIN or any other role that has the privilege to see the query history in Snowsight.  To access query history, click on Activity -> Query History on the left side menu.

NAMING CONVENTIONS
---
The created stream follows the naming convention of `<table name>_STREAM`.  For eg., if the table/view name is `PRODUCT_INFO`, then the created stream will have the name `PRODUCT_INFO_STREAM`.  There are times when you may have table/view names starting with a numeric character - for eg., `"2019_PRODUCT_INFO"`.  The stream name for such an object name will be `"2019_PRODUCT_INFO_STREAM"`.
  
USAGE
---
On the Consumer side, copy and paste the contents of the file in an individual worksheet (NOT PART OF A FOLDER) in Snowsight and run.

CHECKING TO SEE IF STREAMS HAVE BEEN CREATED
---

To check for streams:

```
SHOW STREAMS;
```
If you have enabled streams on table `PRODUCT_INFO` called `PRODUCT_INFO_STREAM`, you can check to see if it contains data:

```
SELECT SYSTEM$STREAM_HAS_DATA('PRODUCT_INFO_STREAM');
```
If it contains data, the above query will return `True`.  If it does not contain data, then it will return `False`.

MISCELLANEOUS
---
This script has been tested a few times in my environment and it works well.  Please inspect the temporary tables and the query history to ensure that change tracking was enabled correctly for all objects in outbound shares as needed.
