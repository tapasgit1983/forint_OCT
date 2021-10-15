# Databricks notebook source
#import builtins
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from datetime import timedelta, timedelta
from pyspark.sql.functions import datediff, to_date, lit,concat, col,date_add, expr, concat, col, lit, row_number, abs, round
from pyspark.sql.window import Window
import re
import pandas as pd
import datetime
from pyspark.sql import Row

existFileList=[]
#Function to Source Blob Storage - Change the container name if your files reside in different conatiner
def connect_data_blob_storage():
  return "/mnt/blob/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
  
def extractDateFromFileName(fileName):  
  y=''
  for x in file_frequncy_list:
    if len(fileName.split(x)) ==2:
      split_file_name=fileName.split(x)
      p=split_file_name[1].strip("-,' ',+")
      y=p[0:8]

  return y

def extractfileFromFileName2(fileName): 
  y=fileName
  for x in file_frequncy_list:
    if len(fileName.split(x)) ==2:
      split_file_name=fileName.split(x)
      p=split_file_name[0]
      y=p+x
  
  return y
  
def restatement_Ind(fileName):
  restated_File_Ind_Msg ='N'

  for key in PatternDictionary:
    if(PatternDictionary[key].match(fileName)):
      restated_File_Ind_Msg ='Y'
      return restated_File_Ind_Msg
  return restated_File_Ind_Msg
  
  
def createDFFromFileList(listFiles):
	rdd1 = sc.parallelize(listFiles)
	row_rdd = rdd1.map(lambda z: Row(Complete_File_Name=z,Extracted_File_Name=extractfileFromFileName2(z),date=extractDateFromFileName(z),restatement_Ind=restatement_Ind(z)))
	df=spark.createDataFrame(row_rdd)
	return df
	
	
def createListFromDF(DF,colno):
	list=[]
	for x in filelistDF:
       list.append(x[colno]).collect()
	return list



if __name__ == __main__:

	file_frequncy_list=['Daily=','Weekly=','Weekly1=','Weekly2=','Weekly3=','Weekly4=','Weekly5=','Weekly6=','Weekly7=']
	PatternDictionary={
			'StatWeeklyWithHistPat': re.compile("(\w+)=(\d+)-(\w+)=(\w+)-(\w+)-(\w+)-(\w+)-(\d){6}(Weekly)=(.*)$"),
			'StatWeeklyWithoutHistpPat':re.compile("(\w+)=(\d+)-(\w+)=(\w+)-(\w+)-(\w+)-(\d){6}(Weekly)=(.*)$"),
			'StatDailyWithHistoryPat':re.compile("(\w+)=(\d+)-(\w+)=(\w+)-(\w+)-(\w+)-(\w+)-(\d){8}(Daily)=(.*)$"),
		}
	blob_path = connect_data_blob_storage()

    #Listing all Files from Container
    listOfFiles = dbutils.fs.ls(blob_path)
    listOfFilesTobeProcessed=[]
    for x in listOfFiles:
    if('xxxxxDaily' in x.name):
      listOfFilesTobeProcessed.append(x.name)
      df=createDFFromFileList(listOfFilesTobeProcessed)



    df1 = df.where(col('restatement_Ind')=='Y')
    sqlContext.registerDataFrameAsTable(df1, "FileNamesTable")
    df_maxdate = sqlContext.sql("select Extracted_File_Name,max(date) as mdate from FileNamesTable group by Extracted_File_Name")
    final_df = (df.alias('df1').join(df_maxdate.alias('df2'),
                               on = df['Extracted_File_Name'] == df_maxdate['Extracted_File_Name'],
                               how = 'inner')).select('df1.*','df2.mdate')
    df_process_ind=final_df.withColumn("process_ind",when(col('date')==col('mdate'),'Y').otherwise('N'))
    df_files_not_to_Be_Processed = df_process_ind.where("process_ind='N'")
    dfFilestobeProcessed = df_process_ind.where("process_ind='Y'")
	print("Total Number of Files - {}".format(df_process_ind.count()))
	print("Total Number of Files to be processed - {}".format(dfFilestobeProcessed.count()))
	print("Total Number of Files not to be processed - {}".format(df_files_not_to_Be_Processed.count()))
	filelistDF = dfFilestobeProcessed.select('Complete_File_Name').collect()
	for x in filelistDF:
		existFileList = createListFromDF(dfFilestobeProcessed,0)
dbutils.notebook.exit(existFileList)

