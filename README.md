# GCP-Practical-task

This app created based on the requirements described in the "Extenda - GCP - Practical task.pdf" file.

Basic description:
This is a backend service running in Cloud Run​, tracking Cloud Storage bucket, and streaming data to BigQuery.

The app uses Application Default Credentials (ADC). 
For local application development, you have to provide authentication credentials by setting the environment variable GOOGLE_APPLICATION_CREDENTIALS.

How to set up the app;
1. Deploy the target jar file to AppEngine and save the target URL.
2. Create a pub/sub topic for the tracked Cloud Storage​ bucket.
3. Create a subscription based on the topic.
- Set the delivery type to "Push"
- Set the Endpoint URL to the previously saved target URL.3
- Set up the subscription filter to track only the object finalize events.
4. Create a new BigQuery dataset named TestDataset.
- Create a table called "Clients" with the next columns:             
      id INTEGER(REQUIRED), name STRING(REQUIRED), phone STRING(NULLABLE), address STRING(NULLABLE) 
- Create a table called "ClientsSubset" with the next columns: 
      id INTEGER(REQUIRED), name STRING(REQUIRED)
- The dataset TestDataset, tables Clients, and ClientsSubset are used by default.
- You can specify another dataset name or table names in the .\src\main\resources\application.properties file.
