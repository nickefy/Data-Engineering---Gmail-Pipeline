# Data-Engineering---Gmail-Pipeline
Gmail Data Pipeline
Automatically extracting, transforming and loading data from your Gmail Inbox into your preferred data warehouse on a daily basis
An automation system that better organises your Gmail attachments into your db. Keep only what you need and scrap the rest 
Easy to use and no hassle. Stop downloading your attachments and uploading it into your data warehouse manually

This repo contains the main operators and the DAG to execute the Pipeline.

Operators to execute the Pipeline in order:
1.Crawl through the Gmail Inbox and download all attachments into GCS
2.Check if there are any attachments to be loaded
3.Load all the attachments into Google Bigquery
4.Checking for any duplication of load in Google Bigquery 
5.Write Logs
6.Send Email