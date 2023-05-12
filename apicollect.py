import requests
import json
import datetime
from urllib.parse import quote
import pandas as pd
import os
from dotenv import load_dotenv
def main():
  load_dotenv()

  reqUrl = os.getenv("ReqUrlLogin")
  rows = []

  headersList = {
  "Accept": "*/*",
  "Content-Type": "application/json"

  }

  payload = json.dumps({
    "UserName": os.getenv("API_USERNAME"),
    "Password": os.getenv("API_PASSWORD")
  })

  response = requests.request("POST", reqUrl, data=payload,  headers=headersList)
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
  response = requests.request("GET", reqUrl, data=payload,  headers=headersList)
  response = json.loads(response.text)
  companyID = response["Data"]
  companyID = companyID[0]["Id"]

  #-------------------------------------

  reqUrl = f'https://app.iolocate.io/api/b2b/companies/{companyID}/devices'
  headersList = {
  "Accept": "*/*",
  "User-Agent": "Thunder Client (https://www.thunderclient.com)",
  "X-ApiToken": AccessToken
  }

  payload = ""
  response = requests.request("GET", reqUrl, data=payload,  headers=headersList)
  dict_data = response.json()
  print(dict_data)
  print(type(dict_data))
  deviceids = [d["Id"] for d in dict_data["Data"]["Source"]]
  print(deviceids)


  to_iso_date_time = datetime.datetime.now()
  from_iso_date_time = to_iso_date_time - datetime.timedelta(minutes=10)

  to_url_encoded_date_time = quote(str(to_iso_date_time))
  from_url_encoded_date_time = quote(str(from_iso_date_time))

  #-------------------------------------
  for device in deviceids:
    reqUrl = f'https://app.iolocate.io/api/b2b/companies/{companyID}/devices/{device}/logs?Page=1&From={from_url_encoded_date_time}&To={to_url_encoded_date_time}'
    headersList = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)",
    "X-ApiToken": AccessToken
    }

    payload = ""
    response = requests.request("GET", reqUrl, data=payload,  headers=headersList)
    response = response.json()

    print(response)
    for source in response['Data']['Source']:
      row = {
          'Asset': source['Asset'],
          'DataLogged': source['DataLogged'],
          'LocationType': source['LocationType'],
          'Latitude': source['Latitude'],
          'Longitude': source['Longitude'],
          'PositionAccuracy': source['PositionAccuracy'],
          'LogReason': source['LogReason'],
          'Battery': source['Battery'],
          # 'BatteryPercentage': source['BatteryPercentage'],
          # 'InternalTemperature': source['InternalTemperature'],
          # 'ExternalTemperature': source['ExternalTemperature'],
          # 'Speed': source['Speed'],
          # 'SpeedAccuracy': source['SpeedAccuracy'],
          # 'Altitude': source['Altitude'],
          # 'HeadingDegrees': source['HeadingDegrees'],
          # 'Alarm': source['Alarm'],
          # 'Angle': source['Angle'],
          # 'RunHours': source['RunHours']
          }
      rows.append(row)


  df = pd.DataFrame(rows)
  df = df.drop_duplicates(subset=['DataLogged'], keep='first')
  df.to_csv('data.csv', index=False)
    
# if __name__ == '__main__':
#     main()