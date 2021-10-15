# Databricks notebook source
pip install azure-cosmosdb-table


from azure.cosmosdb.table.tableservice import TableService
import pandas as pd
import requests
table_service = TableService(account_name='xxxxxxxxxxxxx',account_key="xxxxxxxxxxxxxxxxxxxxxxxxxxxxx") #set Azure connection
data = table_service.query_entities('UserAccountLocationInfo') #read  
df_raw = pd.DataFrame([asset for asset in data])


def regionOfLatLong(latitude, longitude):
    url = "http://postcodes.io/postcodes"
    data = {
    "geolocations" : [{
    "longitude":  longitude,   
    "latitude": latitude,
    "radius": 1000,
    "limit": 1
    }]
    }
    response = requests.post(url, json=data )
    try:
        region = response.json()["result"][0]["result"][0]["region"]
        if region is None:
            region = response.json()["result"][0]["result"][0]["country"]
    except:
        print(response.content)
        return "Unknown on postcodes.io"
        
    return region


def regionOfDevice(macAddress):
    try:
        location = ecoIQtable_service.get_entity(partition_key="Common", row_key=macAddress, table_name="UserAccountLocationInfo")
    except Exception as e:
        print(str(e) + " on " + macAddress)
        return "Unknown"
    
    region = regionOfLatLong(location.Latitude, location.Longitude)
    return region



def regionOfDevice(macAddress):
    try:
        location = table_service.get_entity(partition_key="Common", row_key=macAddress, table_name="UserAccountLocationInfo")
    except Exception as e:
        print(str(e) + " on " + macAddress)
        return "Unknown"
    
    region = regionOfLatLong(location.Latitude, location.Longitude)
    return region



regionOfDevice('06677treewss')




