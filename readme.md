# BCG Case Study
This project uses PySpark to process and analyze large-scale data and is designed to run in a local environment. The primary goal is to load, transform, and analyze data using Spark's distributed computing capabilities while working with configuration settings from a `config.json` file

## Analytics
Application should perform below analysis and store the results for each analysis.
1.	Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
2.	Analysis 2: How many two wheelers are booked for crashes? 
3.	Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
4.	Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
5.	Analysis 5: Which state has highest number of accidents in which females are not involved? 
6.	Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
7.	Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
8.	Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
9.	Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
10.	Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

## Expected Output
1.	Develop an application which is modular & follows software engineering best practices (e.g. Classes, docstrings, functions, config driven, command line executable through spark-submit)
2.	Code should be properly organized in folders as a project
3.	Input data sources and output should be config driven
4.	Code should be strictly developed using Data Frame APIs (Do not use Spark SQL)
5.	Share the entire project as zip or link to project in GitHub repo

Note: All the outputs are stored in the `output` folder within each individual analytics folder

## Features
- Load and parse configurations from a `config.json` file for dynamic processing
- Utilized PySpark to handle large datasets in a distributed manner
- Data transformation capabilities using using Data Frame APIs
- Flexible log path management to track the application's behavior

## Prerequisites
Before starting with this PySpark project, ensure you have the following:
1. Software Setup
    Java: PySpark requires Java Development Kit (JDK). Install JDK version 8 or 11
        a. Verify installation with:
            ```java -version```

    Apache Spark: Download and set up Apache Spark
        a. Download Spark (pre-built for Hadoop)
        b. Set the SPARK_HOME environment variable to your Spark installation path.

   Hadoop (Optional): If you plan to work with HDFS, ensure Hadoop is installed and configured.
2. Python Environment
    Use a package manager like Anaconda or virtualenv to manage dependencies.
    Install PySpark via pip:
       ```pip install pyspark```
3. Environment Variables
    Ensure the following environment variables are configured:
        a. JAVA_HOME: Path to the Java installation
        b. SPARK_HOME: Path to the Spark installation
        c. Add $SPARK_HOME/bin to your system's PATH.
4. Running on Windows
    Download and configure winutils.exe for Hadoop binaries.
    Set the Hadoop home directory:
        set HADOOP_HOME=C:\path\to\winutils

## Installing

Git Link:
```
https://github.com/x-shivaditya/BCG.git
```

### Consideration
The following module was created and tested on Windows

## Steps:
1. Extract the BCG zip folder
2. Go to the Project Directory: `cd bcg`
3. Spark Submit
   ```
   spark-submit main.py --config config.json 
   ```
Note: In case you are replacing the CSV, please extract it from the ZIP file

## Structure
```
BCG
├─ core
│  ├─ __init__.py
│  └─ car_crash_analysis.py
├─ helpers
│  ├─ __init__.py
│  └─ utils.py
├─ logs
│  └─ (Contains multiple log folders)
├─ output
│  ├─ analysis_1
│  ├─ analysis_2
│  ├─ analysis_3
│  ├─ analysis_4
│  ├─ analysis_5
│  ├─ analysis_6
│  ├─ analysis_7
│  ├─ analysis_8
│  ├─ analysis_9
│  └─ analysis_10
├─ resources
│  ├─ BCG_Case_Study_CarCrash_Updated_Questions.docx
│  └─ Data Dictionary.xlsx
├─ src
│  ├─ Charges_use.csv
│  ├─ Damages_use.csv
│  ├─ Endorse_use.csv
│  ├─ Primary_Person_use.csv
│  ├─ Restrict_use.csv
│  ├─ Units_use.csv
│  └─ backup
|     └─ Data.zip   
├─ config.json
└─ readme.md
```
