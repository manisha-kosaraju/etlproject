Initially, created a cluster on databricks for the processing of the data.Then created a new notebook and named it as "etlpipeline".
The dataset WordCount2.text file was uploaded onto the databricks at dbfs:/Filestore/tables/ path. Then the data was extracted using pyspark.
Transformations were performed ie total count of the words in the text file were calculated using the aggregate functions.
I chose to upload the data in postgresSQl database. So for that purpose i created a database at AWS RDS .
Querying the data was done from the pgAdmin. For connectivity with database, we need jdbc connection which requires jdbc drivers. 
So created a connection between pyspark and postgersSQL database using the endpoints. 
A schema has been established in pgAdmin and utilized in PySpark on Databricks for communication purposes.
Finally, loaded the data in the postgresSQl database using "select * from awsrds_schema_pyspark.wordcount".
