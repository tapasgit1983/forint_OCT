# Databricks notebook source
MIN_SUM_THRESHOLD = 10000000
no_of_executors=3


from pyspark.sql.types import StructType,StructField, StringType, FloatType
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import *
def Create_Fact_Table():
  #spark = SparkSession.builder.getOrCreate()
  structureSchema = StructType([ \
    StructField("A", StringType(),False), \
    StructField("B", StringType()),False, \
    StructField("C", FloatType(),False), \
    StructField("D", FloatType(),False), \
    StructField("E", FloatType(), False), \
    StructField("F", FloatType(), True), \
    StructField("G", FloatType(), True), \
    StructField("H", FloatType(), True), \
    StructField("I", FloatType(), True), \
    StructField("J", FloatType(), True) \
  ])
  very_large_dataset_location = '/Sourced/location_1'
  very_large_dataframe = spark.read.csv(very_large_dataset_location, header=True, sep="\t",schema = structureSchema )
  return very_large_dataframe



def Table_Null_Recs_cleanup(data_frame,*cols_for_null_check):
  data_frame.createOrReplaceTempView("DATA")
  str = 'select * from DATA where '
  for x in cols_for_null_check:
    y="{} is null or ".format(x)
    str=str+y
  res = str.strip(' or ')
  df_bad_data = spark.sql(res)
  df_bad_data.write.mode('append').csv('abfss://xxxxxxxxxx@yyyyyyyyyyy.dfs.core.windows.net/ttttttttttt/tttttttt/wwwww/yyyyyy/ffffggggg.csv')
  list_cols = list(cols_for_null_check)
  df_good_records = df.na.drop(subset=list_cols)
  return df_good_records



def column_count(data_frame):
    return len(data_frame.columns)



def has_columns(data_frame, column_name_list):
    list_missing_cols =[]  
    for column_name in column_name_list:
        if(column_name in data_frame.columns) == False:
            list_missing_cols.append(column_name)
            continue
    if len(list_missing_cols)>0:
      raise Exception('Missing columns: ' + str(list_missing_cols).strip('[]'))



def process():
  very_large_dataframe = Create_Fact_Table()
  Table_Null_Recs_cleanup(very_large_dataframe,"A","B","C","D","E")
  
  if column_count(very_large_dataframe) != 10:
    raise Exception('Incorrect column count: ' + str(column_count(very_large_dataframe)))
    
  has_columns(very_large_dataframe, ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'])
  
  number_of_partitions = no_of_executors*4
  
  very_large_dataframe = very_large_dataframe.rapartition(number_of_partitions,"A","B")
  
  very_large_dataframe = very_large_dataframe.withColumn("F",col("F")*2.5)
  
  very_large_dataframe = very_large_dataframe.dropDuplicates(['A'])  
  
  
  #Fact table being a huge table and it is being used to join multiple Dimension tables, table should be persisted to be used in future
  very_large_dataframe=very_large_dataframe.persist(StorageLevel.MEMORY_AND_DISK)
  total_sum_of_column_F = very_large_dataframe.agg(sum('F')).collect()[0][0]
  if total_sum_of_column_F < MIN_SUM_THRESHOLD:
        raise Exception('total_sum_of_column_A: ' + total_sum_of_column_F + ' is below threshold: ' + MIN_SUM_THRESHOLD)
  df_geo_dim=spark.read.format("parquet").load("/location_2") 
  
  # small_geography_dimension_dataframe is small one.Broadcast join shuld be used for joining to Fact Table.Instead of increasing broadcast threshold limit,I would like to go for exclusive broadcast join process as Spark     # size estimator may give overestimate the size of dataframe
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
  very_large_dataframe = (very_large_dataframe.join(broadcast(df_geo_dim),
                                                   (very_large_dataframe.A == df_geo_dim.A),"leftouter").
                                                   select('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J','M'))
  #Product Dimension table is 50 MB dataframe. With Filter size will be further reduced.So, it is small enough to be considered a good candidate for Broadcast join.
  df_prod_dim=spark.read.format("parquet").load("./location_3")
  df_prod_dim= df_prod_dim.filter("col('O')>10")
  very_large_dataframe = very_large_dataframe.join(broadcast(df_prod_dim),(very_large_dataframe.B == df_prod_dim.B),"leftouter").select('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J','M','P')
  
  #coalesce(1) shuld not be done.It should be coalesce(total number of executor).total number of executor = Number of worker nodes*cores in each machine - Number of worker nodes
  very_large_dataframe.coalesce(1)
     .write
     .mode('overwrite')
     .format('com.databricks.spark.csv')
     .option('header', 'true')
     .option('sep', '\t').csv('./location_3'))
  
  



process()




