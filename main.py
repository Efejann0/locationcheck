import apicollect as apicollect
import locationcheck as locationcheck
import database as database
import schedule
import time
import datetime

globaltime = datetime.datetime.now()

while True:
    cnxn,connect_flag = database.connect_mssql()
    if connect_flag == True:
        break

def main():
    global globaltime
    database_data,connect_flag = database.query(cnxn)
    if connect_flag == True:
        merge_apidata, flag ,globaltime= apicollect.main(database_data,globaltime)
    if flag == True:
        locationcheck.main(merge_apidata)
main()
schedule.every(1).minute.do(main)

while True:
    schedule.run_pending()
    time.sleep(1)