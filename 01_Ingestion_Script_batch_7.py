# Databricks notebook source
from pyspark.sql import Row
from datetime import date
import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel

RoomTimeSeriesJsonSchema = StructType([
  StructField("HomeID", StringType(), True),
  StructField("TimeStamp", LongType(), True),
  StructField("TimeZone", StringType(), True),
  StructField("Rooms", ArrayType(StructType([
    StructField("Name", StringType(), True),
    StructField("RoomID", StringType(), True),
    
    StructField("Measurements", ArrayType(StructType([
      StructField("Name", StringType(), True),
      StructField("Value", StringType(), True),
      StructField("Unit", StringType(), True),
    ])), True),
    StructField("mode", StringType(), True),
    StructField("setpointOrigin", StringType(), True)
    
  ])), True)
])




def status_function(mac,status,start_time,end_time,time_diff,Batch_Number): 
  start_time_str = str(start_time)
  end_time_str =str(end_time)
  time_diff_str = str(time_diff)
  insertquery = "insert into RoomTimeSeries.Ingestion_Logs values ('{}','{}','{}','{}','{}','{}')".format(mac,status,start_time_str,end_time_str,time_diff_str,Batch_Number)
  print(insertquery)
  spark.sql(insertquery)
def CreateListFromDFCol(df,col_name):
  Row_List = df.select(col_name).collect()
  list=[]
  for x in Row_List:
    list.append(x[0])
  return list

  
def ReadJson_ConvertDelta(macaddress,Data_load_path,Batch_Number):
  load_path = Data_load_path
  #load_path= "/mnt/rts_par/00_caching_check/00_caching_06062021/Data"
  print("copy will be done for {}".format(macaddress))
  SourceFilePath="/mnt/rts/{}/*/*/*/*".format(macaddress)
  print(SourceFilePath)
  start_time = datetime.datetime.now()
  try:
    
    df = spark.read.schema(RoomTimeSeriesJsonSchema).json(SourceFilePath)
    #df = df.repartition(336,"TimeStamp")
    df1 = df.withColumn("EachRoom",explode("Rooms"))
    df2_persisted = df1.withColumn("RoomName",col("EachRoom.Name"))\
                              .withColumn("RoomID",col("EachRoom.RoomID"))\
                              .withColumn("Mode",col("EachRoom.Mode"))\
                              .withColumn("SetPointOrigin",col("EachRoom.setpointOrigin"))\
                              .withColumn("Room_Temp_Measurement_Details",col("EachRoom.Measurements"))\
                              .drop("Rooms").drop("EachRoom").persist(StorageLevel.MEMORY_AND_DISK)

    df3 = df2_persisted.withColumn("Each_Measurement_Type",explode('Room_Temp_Measurement_Details')).drop('Room_Temp_Measurement_Details')
    df4 = df3.withColumn("Measurement_Name",col("Each_Measurement_Type.Name"))\
               .withColumn("Measurement_Value",col("Each_Measurement_Type.Value"))
    df5 = df4.withColumn("Measurement_Value",col("Measurement_Value").cast('int'))
    df6_persisted = df5.groupBy("HomeID","TimeStamp","RoomName").pivot('Measurement_Name').agg(avg('Measurement_Value')).persist(StorageLevel.MEMORY_AND_DISK)
    join_columns = ['HomeID','RoomName','TimeStamp']
    df7 = df2_persisted.join(broadcast(df6_persisted),on=join_columns,how='inner')
    df8 = df7.drop('Room_Temp_Measurement_Details')
    df_only_necessary_columns = df8.select('HomeID'
                                                ,'TimeStamp'                                    
                                                ,'TimeZone'
                                                ,'RoomName'
                                                ,'RoomID'
                                                ,'Mode'
                                                ,'SetPointOrigin'
                                                ,'BoilerOutput'
                                                ,'MeasuredTemperature'
                                                ,'SetPoint')
    final_df = df_only_necessary_columns\
                              .withColumn("DateTime",from_unixtime(col("TimeStamp")))\
                              .withColumn("Year",year("DateTime"))\
                              .withColumn("Month",month("DateTime"))\
                              .withColumn("Day",dayofmonth("DateTime")).coalesce(84)
    end_time=datetime.datetime.now()
    #time_diff = end_time - start_time
    #status_function(macaddress,'Success Delta Conv and Write',start_time,end_time,time_diff)
  except Exception as e:
    print(str(e))
    end_time=datetime.datetime.now()
    time_diff = end_time - start_time
    status_function(macaddress,'Fail - Read/DF',start_time,end_time,time_diff,Batch_Number)
    return 5
  try:
    (final_df
      .write
      .format("delta")
      .partitionBy("Year","Month","Day")
      .mode('append')
      .save(load_path))
    time_diff = end_time - start_time
    status_function(macaddress,'Success Delta Conv and Write',start_time,end_time,time_diff,Batch_Number)
  except Exception as e:
    print(str(e))
    end_time=datetime.datetime.now()
    time_diff = end_time - start_time
    status_function(macaddress,'Fail-Write',start_time,end_time,time_diff,Batch_Number)
  
  df2_persisted.unpersist()
  df6_persisted.unpersist()
  print("---------------------------------------------------------")


if __name__ == __main__:

	df = spark.read.option('inferSchema',True).option("header",True).csv('/mnt/rts_par/slicedhomeids/Macs_7_1000_06062021.csv')
	l=CreateListFromDFCol(df,'MacAddress')



    DataIngestionPath = "/mnt/rts_par/02_Room_Time_Series_Data_Load/Data"
    Batch_Number ='Batch7'
    for Mac_Address in l:
       ReadJson_ConvertDelta(Mac_Address,DataIngestionPath,Batch_Number)

# COMMAND ----------

print('completed')

