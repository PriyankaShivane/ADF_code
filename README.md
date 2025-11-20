# Incremental Pipeline

This is **Increametal Load Pipleline** which takes takes 3 parameters. All the acivities are handled dynamically.

the Pipeline will ca. also be run for a backdate.

 

 <img width="975" height="619" alt="image" src="https://github.com/user-attachments/assets/8dd05afa-6843-437a-b5f0-bb1f7657cf53" />



<img width="975" height="442" alt="image" src="https://github.com/user-attachments/assets/87294921-afaa-4911-bf18-f8de0e5161ab" />




**Lookup_cdc_date Activity:** will fetch the last proceesed date i.e last_updated date stored in cdc_timestamp.json

 


 

<img width="975" height="651" alt="image" src="https://github.com/user-attachments/assets/92a37b5b-4616-4183-856d-e7594807997b" />





Dataset details of Look_cdc_date Activity

<img width="975" height="502" alt="image" src="https://github.com/user-attachments/assets/b1ff5cd2-2fb6-4c90-b28a-9cf886f97469" />













**Get row count Acitivity:** will count the incremental load rows.


Query : select count(*) as total_count from @{pipeline().parameters.schema}.@{pipeline().parameters.table} where last_updated > '@{if(empty(pipeline().parameters.backdate), activity('Lookup_cdc_date').output.value[0].cdc_timestamp, pipeline().parameters.backdate)}'


   <img width="975" height="651" alt="image" src="https://github.com/user-attachments/assets/d516dc88-22f4-4640-9dc5-87741a849d15" />



**If Condition Acitivity:** will check if there are rows to process or not


Condition used in expression builder: @greater(activity('get_row_count').output.resultSets[0].rows[0].total_count,0)

If true then perform certain activities.

 
<img width="975" height="574" alt="image" src="https://github.com/user-attachments/assets/12e1c6af-ac74-44d0-82ed-ed50fef28edb" />




**Load_Incremental_data Acitivity:** this is a copy activity which Load incremental data to sink from azure sql database.

Source dataset query:

select * from @{pipeline().parameters.schema}.@{pipeline().parameters.table} where last_updated > '@{if(empty(pipeline().parameters.backdate), activity('Lookup_cdc_date').output.value[0].cdc_timestamp, pipeline().parameters.backdate)}'


 
<img width="975" height="669" alt="image" src="https://github.com/user-attachments/assets/008a4675-2d9a-4878-98ff-636ddb291fe1" />


**max_CDC Activity:** get_max_last_updated_date to update the JSON (cdc_timestamp)


select max(last_updated) as cdc_timestamp from @{pipeline().parameters.schema}.@{pipeline().parameters.table}


 <img width="975" height="587" alt="image" src="https://github.com/user-attachments/assets/0052eba5-417c-428b-836f-04d172b99993" />



**load_cdc_timestamp_to_json Activity:** is a copy activity to load cdc_timestamp from empty json to main cdc json.

Source dataset query:

@activity('max_CDC').output.resultSets[0].rows[0].cdc_timestamp


 Sink: Point to main json



<img width="975" height="607" alt="image" src="https://github.com/user-attachments/assets/77bc26eb-0319-4d9a-b179-e12f064117a1" />












 





