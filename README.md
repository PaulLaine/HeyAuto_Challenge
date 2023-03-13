# Vivid Theory - HeyAuto Data Engineering Challenge

### 1 - Introduction

As part of this Data Engineering Challenge, the goal is top apply some fetching, transformation and validation process on the data model used for HeyAuto.

As a reminder here is the different steps required to complete and clean our data model :
- Create a system to fetch the input vehicle data from provided csv url
- Create a system to validate the input vehicle data
- Create a system to transform the input vehicle data (Condition Specifically)
- Console log the output of your program (Accepted Vehicles and Rejected Vehicles)

### 2 - Configuration

I'll complete this challenge by using Python. More specifically, I'll use one of the currently most used parralel processing framework in the industry, Apache Spark.
To do so, I'll convert HeyAuto data into a Spark DataFrame and apply the processing and validation process through the Spark API.

### 3 - Git repository

This repository is composed of 5 files :
- data_heyauto.csv -> Set of data for the challenge
- HeyAuto_DataChallenge.ipynb -> Preliminary and descriptive Python Notebook to process all different steps of the data challenge
- main.py -> Main Python file to process all steps direclty
- validation_functions.py -> Python file containing all validation functions.
- postgres_connector.py -> (Additional) Python file to realise the reading and writing part between PostgreSQL and Spark DataFrame.

### 4 - Result 

To understand the detail of the full process, you can first read the HeyAuto_DataChallenge notebook where every step of the reading, transformation and validation process is described with the result in the table.

When now launching the main.py script, we obtain the following results :

![image](https://user-images.githubusercontent.com/63043011/224767985-329a09a3-83f7-41cf-93ca-13080ddfa39c.png)

We can see that we obtain the accepted and rejected vehicles in 2 different tables and for the rejected vehicles.

### 5 - Additional tasks and Improvement 

##### a. Postgres Connection to read and write.

Being an data storing tool that you are currently using, I added the postgres_connector.py Python file containing functions to automatically pull and push data into PostgreSQL.

With this implementation we could also think of some practices to handle different situation :
- An incremental loading to progessively add every day/week/month/... to our tables, the information of the vehicles that has pass or not the validation process.
- Storing our results into a different system like Microsoft Azure or Snowflake to 

##### b. Automate our workflow with Airflow 

This validation process is something that could be integrated as part of a frequent checking process. The implementation of an automated workflow could save a lot of time on detecting and trigerring actions on the different vehicles that didn't pass the validation process.

A simple DAG could look like this : 

![image](https://user-images.githubusercontent.com/63043011/224770154-d8e3e0b1-afc2-438f-8e93-44ec3f4d84fd.png)

As part of this process, all validation steps are indepedent so we can process them at the same time and merge every results before splitting our dataset into the accepted and rejected vehicles.


