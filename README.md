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

## Deployment of Cloudformation templates

### Parameters handling

These are the necessary parameters to deploy this project, they are in the `/config/test/config.yaml` file

* project_code: name of the project and cloudformation stack
* region: AWS region to deploy this project
* creator: email address of the creator of the deployment (used for resource tagging)

* BKScripts: S3 bucket that holds the Glue ETL scripts
* BKLanding: S3 bucket that hold the folders with the raw data coming from Last FM. This bucket needs to be set up prior the deployment.
* BKRefined: S3 bucket that holds the results once the Glue ETL process has finished.

### Sceptre deployment

The deployment process only needs Sceptre version 2.x. The actual deployment process is started with the `sceptre create test` command.