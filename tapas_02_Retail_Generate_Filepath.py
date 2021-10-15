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
from pyspark.sql.types import *
import time



def Add_Date(retailer,date):
  print("-------------------------Deriving the New file Publishing date-----------------------")
  print("Date  Used for deriving the new publishing date is - {}".format(date)) 
  print("retailerName used for deriving the new publishing date is  {}".format(retailer))
  date_obj = datetime.datetime.strptime(date,'%Y%m%d')
  try:
    add_days_or_week=dateaddDict[retailer]
  except KeyError:
    new_date = date
    print("New File publish Date is - {}".format(new_date))
    return new_date    
    
  x=add_days_or_week.find("_")+1  
  if add_days_or_week.startswith('Week'):    
    new_date_obj=date_obj+timedelta(weeks=int(add_days_or_week[x:]))
  else:
    new_date_obj=date_obj+timedelta(days=int(add_days_or_week[x:]))
    
  new_date=new_date_obj.strftime("%Y%m%d")   
  print("New File publish Date is - {}".format(new_date))
  return new_date    
  


def convert_week_to_date(yearWeek,retailerName,yearWeekDateMappingDF):
    print("-------------------------Deriving the Corresponding Original file Publishing date-----------------------")
    print("YearWeek from file name Used for exracting the publishing date is - {}".format(yearWeek)) 
    print("retailerName used for exracting the publishing date is  {}".format(retailerName))
    
    dateFromMapDF = yearWeekDateMappingDF.where(yearWeekDateMappingDF.Retailer_NAME == retailerName.upper()).filter(yearWeekDateMappingDF.Year_WeekNumber==yearWeek).collect()[0]['Date']
    print("Date from Mapping is {}".format(dateFromMapDF))
     
    objectdateFromDF = datetime.datetime.strptime(dateFromMapDF,'%d/%m/%Y')
    date1=objectdateFromDF.strftime("%Y%m%d")    
    return date1
def fileType_Change(retailer,fileType):
    print("-------------------------Deriving the New FileType-----------------------")
    print("For changing the File type retailer used - {}".format(retailer))
    print("For changing the File type, fileType used - {}".format(fileType))
    x=retailer+"_"+fileType
    try:
      new_fileType = fileTypeDict[x]
    except KeyError:
      new_fileType=fileType
    print("New FileType is - {}".format(new_fileType))
    return new_fileType
      

def splitFileName(fileName):
    splitsOfFileName = re.findall(r"[\w']+", fileName)
    return splitsOfFileName
  
def fileFrequncy(retailer,frequency):
    print("----------------Changing the File Frequency--------------------------")
    print("Existing frequncy is  - {}".format(frequency))
    try:
      filePublishFreq = fileFrequencyChangeDict[retailer]
    except KeyError:
      filePublishFreq = frequency
    print("New frequncy is  - {}".format(filePublishFreq))
    return filePublishFreq
  
def retailerNameforFileNameCompute(retailer):
    print("----------------Changing the Retailer Name--------------------------")
    print("Existing Retailer name is {}".format(retailer))
    try:
      retailerNameChanged = retailerNameChangeDict[retailer]
    except KeyError:
      retailerNameChanged = retailer
    print("New Retailer name is {}".format(retailerNameChanged))
    return retailerNameChanged

def retailerNameforDateCompute(retailer):
    try:
       retailerName4DateMapping = retailerNameForDateComputeDict[retailer]
    except KeyError:
       retailerName4DateMapping=retailer
    return retailerName4DateMapping     
                 


  
def Weekly_Restatement_Name_Modify(fileName):
  
    splits_Restatement = splitFileName(fileName)
    
    print("---------------------Splits of Restatement File------------------")
    print(splits_Restatement)
    splits_Original=[]
    splits_Original.append(splits_Restatement[0])
    splits_Original.append(splits_Restatement[1]) 
    splits_Original.append(splits_Restatement[2])
    splits_Original.append(splits_Restatement[3])
    if(splits_Restatement[4]=='History'):
      splits_Original.append(retailerNameforFileNameCompute(splits_Restatement[5]))
      splits_Original.append(fileType_Change(splits_Restatement[5],splits_Restatement[6]))
      splits_Original.append(fileFrequncy(splits_Restatement[5],splits_Restatement[7][6:]))
      x= retailerNameforDateCompute(splits_Restatement[5])
      try: 
        original_file_publish_date = convert_week_to_date(splits_Restatement[7][0:6],x,DF)
      except:
        dbutils.notebook.exit('{0}/failed_files/{1}'.format(folder_name,fileName))#remove "Date could not be found" before actual execution
      new_original_file_publish_date =Add_Date(splits_Restatement[5],original_file_publish_date)
      splits_Original.append(new_original_file_publish_date)
    else:
      splits_Original.append(retailerNameforFileNameCompute(splits_Restatement[4]))
      splits_Original.append(fileType_Change(splits_Restatement[4],splits_Restatement[5]))
      splits_Original.append(fileFrequncy(splits_Restatement[4],splits_Restatement[6][6:]))
      x= retailerNameforDateCompute(splits_Restatement[4])
      try: 
        original_file_publish_date = convert_week_to_date(splits_Restatement[6][0:6],x,DF)
      except:
        dbutils.notebook.exit('{0}/failed_files/{1}'.format(folder_name,fileName))#remove "Date could not be found" before actual execution
      new_original_file_publish_date =Add_Date(splits_Restatement[4],original_file_publish_date)
      splits_Original.append(new_original_file_publish_date)
      
      
    print("---------------------------Derived splits of New Orginal file---------------------")
    print(splits_Original)
    
    newFileName = splits_Original[0]+"="+splits_Original[1]+"-"+splits_Original[2]+"="+splits_Original[3]+"-"+splits_Original[4]+"-"+splits_Original[5]+"-"+splits_Original[6]+"="+splits_Original[7]
       
  
    return newFileName


def Daily_Restatement_Name_Modify(fileName):
  splits_Restatement = splitFileName(fileName)
  print("---------------------Splits of Restatement File------------------")
  print(splits_Restatement)
  splits_Original=[]
  splits_Original.append(splits_Restatement[0])
  splits_Original.append(splits_Restatement[1]) 
  splits_Original.append(splits_Restatement[2])
  splits_Original.append(splits_Restatement[3])
  splits_Original.append(retailerNameforFileNameCompute(splits_Restatement[5]))
  splits_Original.append(fileType_Change(splits_Restatement[5],splits_Restatement[6]))
  splits_Original.append(splits_Restatement[7][8:])
  original_file_publish_date =splits_Restatement[7][0:8]
  new_original_file_publish_date =Add_Date(splits_Restatement[5],original_file_publish_date)
  splits_Original.append(new_original_file_publish_date)
  print("---------------------------Derived splits of New Orginal file---------------------")
  print(splits_Original) 
  
  newFileName = splits_Original[0]+"="+splits_Original[1]+"-"+splits_Original[2]+"="+splits_Original[3]+"-"+splits_Original[4]+"-"+splits_Original[5]+"-"+splits_Original[6]+"="+splits_Original[7]
  

  return newFileName  


def restatement_File_Name_Change(fileName):
  newFileName=fileName
  restated_File_Ind_Msg ='Not a Restated File'

  for key in PatternDictionary:
    if(PatternDictionary[key].match(fileName)):
      restated_File_Ind_Msg ='a Restated File'
      print("{} is {}".format(fileName,restated_File_Ind_Msg))
      newFileName= PatternFunction[key](fileName)
      return newFileName
  print("{} is {}".format(fileName,restated_File_Ind_Msg))
  return newFileName

def run_with_retry(notebook, timeout, args = {}, max_retries = 3):
  num_retries = 0
  while True:
    try:
      return dbutils.notebook.run(notebook, timeout, args)
    except Exception as e:
      if num_retries > max_retries:
        raise e
      else:
        print("Retrying error", e)
        print(num_retries)
        num_retries += 1



PatternDictionary={
'reStatWeeklyWithHistoryPatt': re.compile("(\w+)=(\d+)-(\w+)=(\w+)-(\w+)-(\w+)-(\w+)-(\d){6}(Weekly)=(.*)$"),
'reStatWeeklyWithoutHistoryPatt':re.compile("(\w+)=(\d+)-(\w+)=(\w+)-(\w+)-(\w+)-(\d){6}(Weekly)=(.*)$"),
'reStatDailyWithHistoryPatt':re.compile("(\w+)=(\d+)-(\w+)=(\w+)-(\w+)-(\w+)-(\w+)-(\d){8}(Daily)=(.*)$"),
}



PatternFunction={
'reStatWeeklyWithHistoryPatt': lambda x:Weekly_Restatement_Name_Modify(x),
'reStatWeeklyWithoutHistoryPatt':lambda x:Weekly_Restatement_Name_Modify(x),
'reStatDailyWithHistoryPatt':lambda x:Daily_Restatement_Name_Modify(x),
}

blob_path = connect_data_blob_storage()
path = "{}/tttttttttttttt_Calendar_mapping.csv".format(blob_path)
print(path)
table_schema = StructType([StructField('Retail_NAME', StringType(), True), 
                           StructField('Year_WeekNumber', StringType(), True),
                           StructField('Date', StringType(), True)])
DF = spark\
    .read\
    .option("header", "true")\
    .schema(table_schema) \
    .option("delimiter",",")\
    .csv(path)
toBeProcessedFileName=   getArgument('file_name')
folder_name          =   getArgument('folder_name')
print(toBeProcessedFileName)
print(folder_name)


print("File to be processed is {}".format(toBeProcessedFileName,))
file_name=restatement_File_Name_Change(toBeProcessedFileName)
print("New File Name is {}".format(file_name))


split2 = re.findall(r"[\w']+", file_name)
print(split2)


pattern = re.compile("(\w+)=(\d+)-(\w+)=(\w+)-(\w+)=(\d+).(\w+)$")
pattern_l = re.compile("(\w+)=(\d+)-(\w+)=(\w+)-(\w+)-(\w+)-(\w+)=(\d+)$")
pattern_w = re.compile("(\w+)=(\d+)-(\w+)=(\w+)-(\w+)-(\w+)-(\w+)-(\w+)=-(\d+)-(\d+)$")
pattern_ws = re.compile("(\w+)=(\d+)-(\w+)=(\w+)-(\w+)-(\w+)-(\w+)-(\w+)=(\d+)-(\d+)$")
pattern_hist = re.compile("(\w+)=(\d+)-(\w+)=(\w+)-(\w+)-(\w+)-(\w+)-(\w+)=(\d+)-$")
pattern_hist_csv = re.compile("(\w+)=(\d+)-(\w+)=(\w+)-(\w+)-(\w+)-(\w+)-(\w+)=(\d+)-.(\w+)$")
pattern_hist_time_csv = re.compile("(\w+)=(\d+)-(\w+)=(\w+)-(\w+)-(\w+)-(\w+)-(\w+)=(\d+)-(\d+).(\w+)$")
pattern_ls = re.compile("(\w+)=(\d+)-(\w+)=(\w+)-(\w+)-(\w+)-(\w+)=(\d+).(\w+)$")
pattern_ppp = re.compile("(\w+)_(\w+)_(\w+)_(\w+)_(\w+)_(\d+)_(\d+)$")
pattern_kkk = re.compile("(\w+)_(\w+)_(\w+)_(\w+)_(\d+)_(\d+)$")



failed_files = {}
now = datetime.datetime.now()
now  = now.strftime("%Y-%m-%d")
print(now)

if bool(pattern_l.search(file_name)):
  print("match the pattern_l")
  splitdatefinal_new = split2[-1]
  cust1_new = split2[4].lower()
  cust_new = split2[5].lower()
  try:
    cust2_new = split2[6].lower()
  except:
    cust2_new = ''
elif bool(pattern_ls.search(file_name)):
  print("match the pattern_ls")
  splitdatefinal_new = split2[-2]
  cust1_new = split2[4].lower()
  cust_new = split2[5].lower()
  try:
    cust2_new = split2[6].lower()
  except:
    cust2_new = ''  
elif bool(pattern.search(file_name)):
  print("match the pattern")
  cust1_new = split2[3].lower()
  cust_new = split2[4].lower()
  cust2_new = ''
  splitdatefinal_new = split2[-2]
elif bool(pattern_w.search(file_name) or pattern_ws.search(file_name)):
  print("match the pattern_w or pattern_ws")
  cust1_new = split2[5].lower()
  cust_new = split2[6].lower()
  cust2_new = split2[7].lower()
  splitdatefinal_new = split2[-2]
elif bool(pattern_hist.search(file_name) and (folder_name == 'retailer')):
  print("match the pattern_hist and retailer folder")
  cust1_new = split2[5].lower()
  cust_new = split2[6].lower()
  cust2_new = split2[7].lower()
  splitdatefinal_new = split2[-1]
elif bool(pattern_hist_csv.search(file_name) and (folder_name == 'retailer')):
  print("match the pattern_hist_csv and retailer folder")
  cust1_new = split2[5].lower()
  cust_new = split2[6].lower()
  cust2_new = split2[7].lower()
  splitdatefinal_new = split2[-2]
elif bool(pattern_hist_time_csv.search(file_name) and (folder_name == 'retailer')):
  print("match the pattern_hist_time_csv and retailer folder")
  cust1_new = split2[5].lower()
  cust_new = split2[6].lower()
  cust2_new = split2[7].lower()
  splitdatefinal_new = split2[-2]
elif bool(pattern_ppp.search(file_name) and (folder_name == 'ppp')):
  print("match the pattern_ppp and ppp folder")
  f = file_name.split("_")
  cust1_new = f[2].lower()
  cust_new = f[4].lower()
  splitdatefinal_new = f[-1]
elif bool(pattern_ppp.search(file_name) and (folder_name == 'ttt')):
  print("match the pattern_ppp and ttt folder")
  f = file_name.split("_")
  cust1_new = f[3].lower()
  cust_new = f[4].lower()
  splitdatefinal_new = f[-1]
elif bool(pattern_kkk.search(file_name) and (folder_name == 'ppp')):
  print("match the pattern_kkk and ppp folder")
  f = file_name.split("_")
  cust1_new = f[2].lower()
  cust_new = f[3].lower()
  splitdatefinal_new = f[-1]
elif bool(pattern_kkk.search(file_name) and (folder_name == 'kkk')):
  print("match the pattern_kkk and kkk folder")
  f = file_name.split("_")
  cust1_new = f[2].lower()
  if (f[3].upper() == 'kkk'): 
    cust_new = 'Data'
  else: 
    cust_new = f[3]
  splitdatefinal_new = f[-1]
else:
  try:
    dbutils.notebook.exit('{0}/failed_files/{1}'.format(folder_name,file_name))
  except:
    None

try:
  splitdatefinal_str=splitdatefinal_new[0:8]
  Year = splitdatefinal_str[0:4]
  Month = splitdatefinal_str[4:6]
  Day = splitdatefinal_str[6:8]
  timestamp = splitdatefinal_new[8:]
except:
  splitdatefinal_new=splitdatefinal_new[0:8]
  Year = splitdatefinal_new[0:4]
  Month = splitdatefinal_new[4:6]
  Day = splitdatefinal_new[6:8]


print(splitdatefinal_str)
print(timestamp)





hist = ['hist','History','Hist']

todayYMD = (datetime.datetime.now()).strftime('%Y%m%d')
if (any(x in file_name for x in hist) and (folder_name=='retailer')):
  OutputPathxxxopenraw= ("{3}/history/{0}/{1}/{2}/" + Year + "/" + Month + "/" + Day + "/" + "{0}_" + str(int(time.time())) +
                        ".csv").format(cust1_new,cust_new,cust2_new,folder_name)
elif (any(x in file_name for x in hist) and (folder_name in ['kkk','ttt','ppp'])):
  OutputPathxxxopenraw= ("{2}/history/{0}/{1}/" + Year + "/" + Month + "/" + Day + "/" + "{0}_" + str(int(time.time())) +
                        ".csv").format(cust1_new,cust_new,folder_name)
elif (folder_name in ['kkk','ttt','ppp']):
  OutputPathxxxopenraw= ("{2}/{0}/{1}/" + Year + "/" + Month + "/" + Day + "/" + "{0}_" + str(int(time.time())) + timestamp + ".csv").format(cust1_new,cust_new,folder_name)
else:
  OutputPathxxxopenraw= ("{3}/{0}/{1}/{2}/" + Year + "/" + Month + "/" + Day + "/" + "{0}_" + str(int(time.time())) +
                        ".csv").format(cust1_new,cust_new,cust2_new,folder_name)
print(OutputPathxxxopenraw)




x=run_with_retry("xxxOpen_03_Delete_Existing_CSV_Files", 300, {"Path": OutputPathxxxopenraw},max_retries = 4)
print(x)
y=x.split(",")
if len(y)==1:
  print("Delete Path Not available - {}".format(y[0]))
else:
  print("Number of CSV Files Moved is - {}.".format(y[0]))
  print("Number of Non - CSV Files or Folders - {}.So they were not required to be moved.".format(y[1]))


dbutils.notebook.exit(OutputPathxxxopenraw)



