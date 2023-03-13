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
- validation_functions.py -> Python file containing all validation functions
- postgres_connector.py -> (Additional) Python file to realise the reading and writing part between PostgreSQL and Spark DataFrame

### 4 - Result 

To understand the detail of the full process, you can first read the HeyAuto_DataChallenge notebook.

To now 
