import requests
import json
import datetime
from urllib.parse import quote
import pandas as pd
import os
import pytz
import math
from dotenv import load_dotenv

def main(data,globaltime):
  logperiod = float(1440)
  minutes = math.floor(logperiod)
  seconds = (logperiod - minutes) * 60 
  load_dotenv()
  reqUrl = os.getenv("ReqUrlLogin")
  rows = []
  flag = False
  headersList = {
  "Accept": "*/*",
  "Content-Type": "application/json"

  }


  payload = json.dumps({
    "UserName": os.getenv("API_USERNAME"),
    "Password": os.getenv("API_PASSWORD")
  })

  try:
    response = requests.request("POST", reqUrl, data=payload,  headers=headersList)

  except Exception as e:
    print("The error is login: ",e)
    merged_dataframe = pd.DataFrame()
    flag = False
    return merged_dataframe ,flag, globaltime
  
  response = json.loads(response.text)
  AccessToken = response["Data"]["AccessToken"]
  #-------------------------------------

  headersList = {
  "Accept": "*/*",
  "User-Agent": "Thunder Client (https://www.thunderclient.com)",
  "X-ApiToken": AccessToken
  }
  payload = f'"X-ApiToken": {AccessToken}'
  reqUrl =  os.getenv("ReqUrlCompanies")
  try:
    response = requests.request("GET", reqUrl, data=payload,  headers=headersList)

  except Exception as e:
    print("The error is ApiToken: ",e)
    merged_dataframe = pd.DataFrame()
    flag = False
    return merged_dataframe ,flag, globaltime
  
  response = json.loads(response.text)
  companyID = response["Data"]
  companyID = companyID[0]["Id"]

  #-------------------------------------
  to_iso_date_time = globaltime
  from_iso_date_time = to_iso_date_time - datetime.timedelta(minutes=minutes, seconds=seconds)

  print(from_iso_date_time,' ', to_iso_date_time)

  to_iso_date_time = to_iso_date_time.replace(tzinfo=pytz.utc)
  to_url_encoded_date_time = to_iso_date_time.isoformat(timespec='microseconds')
  to_url_encoded_date_time = to_url_encoded_date_time.replace('+00:00', 'Z')
  from_iso_date_time = from_iso_date_time.replace(tzinfo=pytz.utc)
  from_url_encoded_date_time = from_iso_date_time.isoformat(timespec='microseconds')
  from_url_encoded_date_time = from_url_encoded_date_time.replace('+00:00', 'Z')

  to_url_encoded_date_time = quote(str(to_url_encoded_date_time))
  from_url_encoded_date_time = quote(str(from_url_encoded_date_time))
  
  #-------------------------------------
  for value in data['yabby_kod']:
    reqUrl = f'https://app.iolocate.io/api/b2b/companies/{companyID}/devices/{value}/logs/dates?Page=1&From={from_url_encoded_date_time}&To={to_url_encoded_date_time}'
    headersList = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)",
    "X-ApiToken": AccessToken
    }

    payload = ""
    try:
      response = requests.request("GET", reqUrl, data=payload,  headers=headersList)

    except Exception as e:
        print("The error is device location: ",e)
        merged_dataframe = pd.DataFrame()
        flag = False
        return merged_dataframe ,flag, globaltime

    response = response.json()

    for source in response['Data']['Source']:
      row = {
          'yabby_kod': source['AssetId'],
          'Asset': source['Asset'],
          'DataLogged': source['DataLogged'],
          'Latitude': source['Latitude'],
          'Longitude': source['Longitude'],
          'PositionAccuracy': source['PositionAccuracy'],
          'LogReason': source['LogReason'],
          'Battery': source['Battery']
          }
      rows.append(row)


  df = pd.DataFrame(rows)
  df = df.drop_duplicates(subset=['DataLogged'], keep='first')
  # df.to_csv('data.csv', index=False)
  try:
    merged_dataframe = pd.merge(df, data, on='yabby_kod', how='inner')
    flag = True
    globaltime = globaltime + datetime.timedelta(minutes=minutes, seconds=seconds)
    return merged_dataframe ,flag, globaltime
  except KeyError:
    merged_dataframe = pd.DataFrame()
    flag = False
    globaltime = globaltime + datetime.timedelta(minutes=minutes, seconds=seconds)
    return merged_dataframe ,flag, globaltime