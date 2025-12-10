Incremental Load Pipeline using Azure Data Factory (ADF)

**Description:**

Designed and implemented a robust Incremental Load Pipeline in Azure Data Factory to efficiently process delta records from Azure SQL Database into the data lake. The pipeline is parameter-driven and optimized to ensure only new or updated data is ingested, significantly improving performance and reducing processing costs.


<img width="975" height="442" alt="image" src="https://github.com/user-attachments/assets/04a74d5d-2203-41f9-8be7-46c80b4a512f" />



 

**Impact & Outcomes**

✔ Improved pipeline efficiency by avoiding full loads

✔ Reduced execution time and compute cost

✔ Enabled scalable ingestion across multiple tables

✔ Ensured accurate, consistent CDC (Change Data Capture) tracking

✔ Fully automated and production-ready incremental data ingestion flow



**Key Features & Workflow**

🔹 Parameter-Driven Pipeline

The pipeline accepts three parameters:

1. Schema

2.	Table Name

3.	Backdate (optional)


This enables dynamic execution across multiple tables without code changes.

 

<img width="975" height="647" alt="image" src="https://github.com/user-attachments/assets/cfb0d4e0-b84a-440e-a032-faf36fd8dff5" />






	A **Lookup activity** retrieves the previously processed last_updated value stored in cdc_timestamp.json, ensuring accurate incremental capture.



<img width="975" height="651" alt="image" src="https://github.com/user-attachments/assets/8e07a7df-f7d7-4fee-8264-4fcafad62f12" />


 

DataSet used for Lookup activity.


 

<img width="975" height="502" alt="image" src="https://github.com/user-attachments/assets/9724d059-790e-45e7-80e8-42a29320df9e" />















**	Get_row_count Acitivity | Row Count Verification :**


A SQL query mentioned in get_row_count checks whether new rows exist and will count the incremental load rows.

SQL query:

_select count(*) as total_count from @{pipeline().parameters.schema}.@{pipeline().parameters.table} where last_updated > '@{if(empty(pipeline().parameters.backdate), activity('Lookup_cdc_date').output.value[0].cdc_timestamp, pipeline().parameters.backdate)}'_




<img width="975" height="689" alt="image" src="https://github.com/user-attachments/assets/10a3bdbb-848a-464d-958a-9f50a73ce020" />



**	If Condition Activity**

An If Condition activity validates if total_count > 0 using below mentioned expression.
Only then the incremental load flow is triggered.

Expression:

_@greater(activity('get_row_count').output.resultSets[0].rows[0].total_count,0)_


If true then perform below mentioned activities.

 


<img width="975" height="612" alt="image" src="https://github.com/user-attachments/assets/afa98e54-0773-447e-86d6-f56fe5fd7e2d" />




**🔹 Load_Incremental_data Acitivity | Incremental Data Copy**


A Copy activity loads only the delta records based on the latest last_updated timestamp into the designated sink (data lake or target zone).
Source query dynamically evaluates either the stored timestamp or the optional backdate.


Source dataset query:

_select * from @{pipeline().parameters.schema}.@{pipeline().parameters.table} where last_updated > '@{if(empty(pipeline().parameters.backdate), activity('Lookup_cdc_date').output.value[0].cdc_timestamp, pipeline().parameters.backdate)}'_


 

<img width="975" height="695" alt="image" src="https://github.com/user-attachments/assets/85269b3f-5aa4-4eb2-ae85-6d6c3edebe32" />


 

**🔹 max_CDC Activity | CDC Timestamp Update Logic**

After the load is complete:

•	A query calculates max(last_updated) from the source table.
•	The value is written back to cdc_timestamp.json using a Copy activity to ensure the next run processes only newer records.

Query:

_select max(last_updated) as cdc_timestamp from @{pipeline().parameters.schema}.@{pipeline().parameters.table}_




<img width="975" height="708" alt="image" src="https://github.com/user-attachments/assets/654450a8-4f12-455c-a6eb-c1903b2bfc00" />

  

**🔹 load_cdc_timestamp_to_json** : is a copy activity to load cdc_timestamp from empty json to main cdc json.

Source dataset query:

_@activity('max_CDC').output.resultSets[0].rows[0].cdc_timestamp_

  





<img width="975" height="727" alt="image" src="https://github.com/user-attachments/assets/982505cc-1aa5-4ac7-97b3-97254bfba88e" />












**Sink: Point to main json.**



<img width="975" height="644" alt="image" src="https://github.com/user-attachments/assets/b3f1d8d2-53ab-4549-a28e-b6ff3c2636c5" />

 





