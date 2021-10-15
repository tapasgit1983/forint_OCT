# Databricks notebook source
import os
from pyspark.sql.functions import col, asc
from datetime import datetime
from datetime import timedelta
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
import io
from azure.identity import ClientSecretCredential
from dateutil import tz


def file_path_creation(TimeZone,DeltaDays):
    now=datetime.now()
    GET_TimeZone = tz.gettz(TimeZone)
    now_Time_Zoned = now.astimezone(GET_TimeZone)
    now_formatted=now_Time_Zoned.strftime("%Y%m%d-%H%M%S")



#Prepare variables for dynamic path to search latest eRetailer folder based on previous day
    print("Today date is: " + now_Time_Zoned.strftime("%d/%m/%Y"))
    days_to_substract = timedelta(days=DeltaDays)

    previous_day = now_Time_Zoned - days_to_substract
    previous_day_str =previous_day.strftime("%d/%m/%Y")
    print("Yesterday day was: {}".format(previous_day_str))

    previous_day_filePath = previous_day.strftime("%Y/%m/%d")
    print("File Path for Previous Day's file is : {}".format(previous_day_filePath))


    partner_File_path = "abfss://gggggg@ttttttttttttttttt.dfs.core.windows.net/gggggggggggg/bbbbbbb/jjjjjjjjjj/mmm/iiiiii/{}/".format(previous_day_filePath)
	return partner_File_path

retail_File_existence_Ind = 'N'
if __name__== __main__:
    partner_File_path = file_path_creation('CET',1)
	list_of_partner_Files  = dbutils.fs.ls(partner_File_path)
	
    for x in list_of_partner_Files:
      if(x.name == 'UK_ggggg_SALES.csv.bz2'):
          retail_File_existence_Ind = 'Y'




dbutils.notebook.exit(retail_File_existence_Ind)



