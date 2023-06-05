import apicollect as apicollect
import locationcheck as locationcheck
import database as database
import schedule
import time
import datetime

last_append_date = None
flag = False

while True:
    cnxn,connect_flag = database.connect_mssql()
    last_append_date = database.take_last_append_date_postgresql()
    if connect_flag == True:
        break

def main():
    global last_append_date , flag
    database_data,connect_flag = database.query(cnxn)
    if connect_flag == True:
        merge_apidata, flag= apicollect.main(database_data,last_append_date)
    if flag == True:
        last_append_date = locationcheck.main(merge_apidata)
main()
schedule.every(55).seconds.do(main)

while True:
    schedule.run_pending()
    time.sleep(1)