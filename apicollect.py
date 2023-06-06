import requests
import json
import datetime
from urllib.parse import quote
import pandas as pd
import os
import pytz
from dotenv import load_dotenv
source = None

def main(data,last_append_dates):
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
    return merged_dataframe ,flag
  
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
    return merged_dataframe ,flag
  
  response = json.loads(response.text)
  companyID = response["Data"]
  companyID = companyID[0]["Id"]

  #-------------------------------------
  # from_iso_date_time = last_append_dates
  # to_iso_date_time = datetime.datetime.now()

  # print(from_iso_date_time,' ', to_iso_date_time)

  # to_iso_date_time = to_iso_date_time.replace(tzinfo=pytz.utc)
  # to_url_encoded_date_time = to_iso_date_time.isoformat(timespec='microseconds')
  # to_url_encoded_date_time = to_url_encoded_date_time.replace('+00:00', 'Z')
  # from_iso_date_time = from_iso_date_time.replace(tzinfo=pytz.utc)
  # from_url_encoded_date_time = from_iso_date_time.isoformat(timespec='microseconds')
  # from_url_encoded_date_time = from_url_encoded_date_time.replace('+00:00', 'Z')

  # to_url_encoded_date_time = quote(str(to_url_encoded_date_time))
  # from_url_encoded_date_time = quote(str(from_url_encoded_date_time))
  #-------------------------------------
  for value in data['yabby_kod']:
    last_append_date_frame = last_append_dates.loc[(last_append_dates['yabby_kod'] == value), ['datalogged']]
    if not last_append_date_frame.empty:
      last_append_date_value = last_append_date_frame['datalogged'].loc[last_append_date_frame.index[0]]
      from_iso_date_time = last_append_date_value
      to_iso_date_time = datetime.datetime.now()

      print(from_iso_date_time,' ', to_iso_date_time,' ', value)

      to_iso_date_time = to_iso_date_time.replace(tzinfo=pytz.utc)
      to_url_encoded_date_time = to_iso_date_time.isoformat(timespec='microseconds')
      to_url_encoded_date_time = to_url_encoded_date_time.replace('+00:00', 'Z')
      from_iso_date_time = from_iso_date_time.replace(tzinfo=pytz.utc)
      from_url_encoded_date_time = from_iso_date_time.isoformat(timespec='microseconds')
      from_url_encoded_date_time = from_url_encoded_date_time.replace('+00:00', 'Z')

      to_url_encoded_date_time = quote(str(to_url_encoded_date_time))
      from_url_encoded_date_time = quote(str(from_url_encoded_date_time))

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
          return merged_dataframe ,flag

      response = response.json()
      # print(response)
  try:
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
          
          if source['Latitude'] and source['Longitude']:
              rows.append(row)
          else:
              print("Skipping empty latitude/longitude.")
  except Exception as e:
      print("response['Data']['Source']: ", e)

  df = pd.DataFrame(rows)
  df = df.drop_duplicates(subset=['DataLogged'], keep='first')
  # df.to_csv('data.csv', index=False)
  try:
    merged_dataframe = pd.merge(df, data, on='yabby_kod', how='inner')
    flag = True
    return merged_dataframe ,flag
  except KeyError:
    merged_dataframe = pd.DataFrame()
    flag = False
    return merged_dataframe ,flag