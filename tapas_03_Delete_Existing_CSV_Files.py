# Databricks notebook source
import datetime
def connect_data_lake_gen_1():
  spark.conf.set("fs.adl.oauth2.access.token.provider.type", "ClientCredential")
  spark.conf.set("fs.adl.oauth2.client.id", "xxxxxxxxxxxxxxxxxxxxxxx")
  spark.conf.set("fs.adl.oauth2.credential", "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
  spark.conf.set("fs.adl.oauth2.refresh.url", "tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt")
  return "adl://ggggggggggggggggggggggggg.azuredatalakestore.net/INNOVATION/ttttttttttttttttttttttt/TapasTest/uuuuuuuuuuuuuuuuuuuuuuuuuuu"





#Connecting to Gen 1 and taking root folder path
root_folder = connect_data_lake_gen_1()

filePath = getArgument("Path")
print(filePath)

#It will just derive the directory where the file will be residing - Root Folder + (Parameter path minus file name)
splitFilePath = filePath.split("/")
dirTobeCleaned = "/"
for i in range(0,len(splitFilePath)-1):
  dirTobeCleaned = dirTobeCleaned + splitFilePath[i]+"/"
pathForDeleteActivity = root_folder+dirTobeCleaned
print("Path for Clean Activity {}".format(pathForDeleteActivity))

#List the files in Directory where delete activity will happen.if there is no file in that directory then take exit
try:
  listOfFiles = dbutils.fs.ls(pathForDeleteActivity)
  print(listOfFiles)
except:
  dbutils.notebook.exit(pathForDeleteActivity)

#Creating the Trash Folder Path
TrashFolder = root_folder+"/"+splitFilePath[0]+"/"+"Trash/"
TrashFolder

newNameForFileToBeMoved = dirTobeCleaned.replace("/","_")
newNameForFileToBeMoved = newNameForFileToBeMoved.strip("_")
newNameForFileToBeMoved


now=datetime.datetime.now()
now_formatted=now.strftime("%Y%m%d-%H%M%S")


i=0
j=0
for x in listOfFiles:
  if(x.isFile() & x.path.endswith('.csv')):
    fromPath=x.path
    toPath=TrashFolder+newNameForFileToBeMoved+"_"+"Trash_Time_"+now_formatted+"-"+x.name
    print("From Path - {}".format(fromPath))
    print("To Path - {}".format(toPath))
    dbutils.fs.mv(fromPath,toPath)
    
    i+=1
  else:
    j+=1
print("Number of CSV Files Moved is {}".format(i))
print("Number of Non - CSV Files or Folders {}.So they were not required to be moved".format(j))
exitList="{},{}".format(i,j)

dbutils.notebook.exit(exitList)



