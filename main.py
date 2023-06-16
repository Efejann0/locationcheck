import apicollect as apicollect
import locationcheck as locationcheck
import database as database
import schedule
import time

last_append_dates = None
flag = None

while True:
    cnxn,connect_flag = database.connect_mssql()
    last_append_dates = database.take_last_append_date_postgresql()
    if connect_flag == True:
        break

def main():
    global last_append_dates , flag
    database_data,connect_flag = database.query(cnxn)
    if connect_flag == True:
        merge_apidata, flag= apicollect.main(database_data,last_append_dates)
    if flag == True:
        last_append_dates= locationcheck.main(merge_apidata)
    print("Bir dongu tamamlandi")  
    # eliar_database_enddate = database.eliar_enddate()
    # df_factory_enddate = database.factory_enddate(cnxn)
    # database.merge_and_update_enddate(eliar_database_enddate, df_factory_enddate)
main()
schedule.every(55).seconds.do(main)

while True:
    schedule.run_pending()
    time.sleep(1)