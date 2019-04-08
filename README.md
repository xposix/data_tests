# Last FM Data Analytics Exercises

This project contains the code to solve the three Data Analytics exercises proposed against the Last FM dataset.
Unfortunately, I ran out of time so I could not finish my original idea, which was running the Spark code in AWS Glue ETL and generate the different results on S3 buckets.

## Status of the code

At the moment there are two separated source codes

* IaC solution using Cloudformation and Sceptre that works by itself what deploys the following:
  * Necessary IAM roles
  * AWS Glue Database
  * AWS Glue ETL job
  * AWS Athena Named Queries for the Athena tables generation

* PySpark scripts. Lack of time stopped me from moving the two first exercises to Dataframes. I consider them more efficient for that purpose.
  * Level1: working solution based on RDDs.
  * Level2: working solution based on RDDs.
  * Level3: it works for smaller files but it runs out of memory when trying to handle the original 2.4 GB file.

## Content of the repo

* /config: sceptre environment config to deploy the Cloudformation templates
* /glue_scripts: Glue ETL script for a basic transformation.
* /last.fm: PySpark scripts for the three exercises
* /templates: Cloudformation template for the deployment of the ETL solution

