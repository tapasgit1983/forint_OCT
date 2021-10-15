# Databricks notebook source
dbutils.widgets.text('file_name','','')
dbutils.widgets.text('folder_name','','')



from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from datetime import datetime, timedelta, timedelta
from pyspark.sql.functions import datediff, to_date, lit,concat, col,date_add, expr, concat, col, lit, row_number, abs, round
from pyspark.sql.window import Window
import re
import pandas as pd
import time

file_name=getArgument('file_name')
folder_name = getArgument('folder_name')
print(file_name)
print(folder_name)

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
pattern_pqr = re.compile("(\w+)_(\w+)_(\w+)_(\w+)_(\w+)_(\d+)_(\d+)$")
pattern_cdd = re.compile("(\w+)_(\w+)_(\w+)_(\w+)_(\d+)_(\d+)$")


failed_files = {}
now = datetime.now()
now  = now.strftime("%Y-%m-%d")


if bool(pattern_l.search(file_name)):
  splitdatefinal_new = split2[-1]
  cust1_new = split2[4].lower()
  cust_new = split2[5].lower()
  try:
    cust2_new = split2[6].lower()
  except:
    cust2_new = ''
elif bool(pattern_ls.search(file_name)):
  splitdatefinal_new = split2[-2]
  cust1_new = split2[4].lower()
  cust_new = split2[5].lower()
  try:
    cust2_new = split2[6].lower()
  except:
    cust2_new = ''  
elif bool(pattern.search(file_name)):
  cust1_new = split2[3].lower()
  cust_new = split2[4].lower()
  cust2_new = ''
  splitdatefinal_new = split2[-2]
elif bool(pattern_w.search(file_name) or pattern_ws.search(file_name)):
  cust1_new = split2[5].lower()
  cust_new = split2[6].lower()
  cust2_new = split2[7].lower()
  splitdatefinal_new = split2[-2]
elif bool(pattern_hist.search(file_name) and (folder_name == 'retailer')):
  cust1_new = split2[5].lower()
  cust_new = split2[6].lower()
  cust2_new = split2[7].lower()
  splitdatefinal_new = split2[-1]
elif bool(pattern_hist_csv.search(file_name) and (folder_name == 'retailer')):
  cust1_new = split2[5].lower()
  cust_new = split2[6].lower()
  cust2_new = split2[7].lower()
  splitdatefinal_new = split2[-2]
elif bool(pattern_hist_time_csv.search(file_name) and (folder_name == 'retailer')):
  cust1_new = split2[5].lower()
  cust_new = split2[6].lower()
  cust2_new = split2[7].lower()
  splitdatefinal_new = split2[-2]
elif bool(pattern_pqr.search(file_name) and (folder_name == 'pqr')):
  f = file_name.split("_")
  cust1_new = f[2].lower()
  cust_new = f[4].lower()
  splitdatefinal_new = f[-1]
elif bool(pattern_pqr.search(file_name) and (folder_name == 'mio')):
  f = file_name.split("_")
  cust1_new = f[3].lower()
  cust_new = f[4].lower()
  splitdatefinal_new = f[-1]
elif bool(pattern_cdd.search(file_name) and (folder_name == 'pqr')):
  f = file_name.split("_")
  cust1_new = f[2].lower()
  cust_new = f[3].lower()
  splitdatefinal_new = f[-1]
elif bool(pattern_cdd.search(file_name) and (folder_name == 'cdd')):
  f = file_name.split("_")
  cust1_new = f[2].lower()
  if (f[3].upper() == 'cdd'): 
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






hist = ['hist','History','Hist']

todayYMD = (datetime.now()).strftime('%Y%m%d')
if (any(x in file_name for x in hist) and (folder_name=='retailer')):
  OutputPathe2openraw= ("{3}/history/{0}/{1}/{2}/" + Year + "/" + Month + "/" + Day + "/" + "{0}_" + str(int(time.time())) +
                        ".csv").format(cust1_new,cust_new,cust2_new,folder_name)
elif (any(x in file_name for x in hist) and (folder_name in ['cdd','bbb','pqr'])):
  OutputPathe2openraw= ("{2}/history/{0}/{1}/" + Year + "/" + Month + "/" + Day + "/" + "{0}_" + str(int(time.time())) +
                        ".csv").format(cust1_new,cust_new,folder_name)
elif (folder_name in ['cdd','bbb','pqr']):
  OutputPathe2openraw= ("{2}/{0}/{1}/" + Year + "/" + Month + "/" + Day + "/" + "{0}_" + str(int(time.time())) + timestamp + ".csv").format(cust1_new,cust_new,folder_name)
else:
  OutputPathe2openraw= ("{3}/{0}/{1}/{2}/" + Year + "/" + Month + "/" + Day + "/" + "{0}_" + str(int(time.time())) +
                        ".csv").format(cust1_new,cust_new,cust2_new,folder_name)
print(OutputPathe2openraw)

dbutils.notebook.exit(OutputPathe2openraw)