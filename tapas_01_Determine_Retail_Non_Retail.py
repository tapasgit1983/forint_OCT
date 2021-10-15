# Databricks notebook source
dbutils.widgets.text("file_name","xx")
dbutils.widgets.text("folder_name","xx")

# COMMAND ----------

file_name=   getArgument('file_name')
folder_name          =   getArgument('folder_name')
print(file_name)
print(folder_name)

# COMMAND ----------

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

# COMMAND ----------

if folder_name=='retailer':
  print("Retail Execution....")
  x=run_with_retry("tapas_02_Retail_Generate_Filepath", 300, {"file_name": file_name,"folder_name":folder_name},max_retries = 4)
else:
  print("Non  - Retail Execution....")
  x=run_with_retry("tapas_02_nonRetail_filePath", 300, {"file_name": file_name,"folder_name":folder_name},max_retries = 4)

# COMMAND ----------

dbutils.notebook.exit(x)