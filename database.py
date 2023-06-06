from sqlalchemy import create_engine, text
import pandas as pd
import psycopg2
from psycopg2 import Error
from dotenv import load_dotenv
import os

connect_flag = False
dummy_conn = 'just for return'

def query(engine):
    with engine.connect() as connection:
        try:
            sql_query = text("""SELECT CINSI, SIPNO, SIRA, PARKAYNO, 
                            KUMNO, PARKUMNO, MUSTERI, KUMAS, MIKTAR,
                            TAMIR_NEDENI, ARGE_yabby, BITTI, yabby_kod,
                            CREATEDATE,	Yabby_Aktif, ENDDATE
                            FROM ABP.dbo.v_parkum_yabby where Yabby_Aktif = 1
                            and yabby_kod is not null;""")
            result = connection.execute(sql_query)
            results = result.fetchall()
            df = pd.DataFrame(results)
            results_cleaned = [tuple(element.strip() if isinstance(element, str) else element for element in tup) for tup in results]
            df = pd.DataFrame(results_cleaned, columns=['CINSI', 'SIPNO', 'SIRA', 'PARKAYNO', 
                                                        'KUMNO', 'PARKUMNO', 'MUSTERI', 'KUMAS', 'MIKTAR',
                                                        'TAMIR_NEDENI', 'ARGE_yabby', 'BITTI', 'yabby_kod',
                                                        'CREATEDATE',	'Yabby_Aktif', 'ENDDATE'])
            connect_flag = True
            return df,connect_flag
        except Exception as e:
            connect_flag = False
            print("The error is connect_mssql: ",e)
            return dummy_conn ,connect_flag
        
def end_date(engine):
    with engine.connect() as connection:
        try:
            sql_query_end = text("""SELECT CINSI, SIPNO, SIRA, PARKAYNO, 
                                KUMNO, PARKUMNO, MUSTERI, KUMAS, MIKTAR, TAMIR_NEDENI, 
                                ARGE_yabby, BITTI, yabby_kod,
                                CREATEDATE,	Yabby_Aktif, max(ENDDATE) as ENDDATE
                                FROM ABP.dbo.v_parkum_yabby where Yabby_Aktif = 0
                                and yabby_kod is not null 
                                GROUP BY CINSI, SIPNO, SIRA, PARKAYNO, KUMNO,
                                PARKUMNO, MUSTERI, KUMAS, MIKTAR, TAMIR_NEDENI, 
                                ARGE_yabby, BITTI, yabby_kod,
                                CREATEDATE,	Yabby_Aktif, ENDDATE
                                ORDER BY ENDDATE DESC;""")
            result = connection.execute(sql_query_end)
            results = result.fetchall()
            df = pd.DataFrame(results)
            results_cleaned = [tuple(element.strip() if isinstance(element, str) else element for element in tup) for tup in results]
            df = pd.DataFrame(results_cleaned, columns=['CINSI', 'SIPNO', 'SIRA', 'PARKAYNO', 
                                                        'KUMNO', 'PARKUMNO', 'MUSTERI', 'KUMAS', 'MIKTAR',
                                                        'TAMIR_NEDENI', 'ARGE_yabby', 'BITTI', 'yabby_kod',
                                                        'CREATEDATE',	'Yabby_Aktif', 'ENDDATE'])
            connect_flag = True
            return df, connect_flag
        except Exception as e:
            connect_flag = False
            print("The error is connect_mssql: ",e)
            return dummy_conn ,connect_flag

def connect_mssql():
    load_dotenv()
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')

    try:
        connect_flag = True
        connection_string = f'mssql+pymssql://{username}:{password}@{server}/{database}'
        engine = create_engine(connection_string)
        return engine , connect_flag 
    except Exception as e:
        connect_flag = False
        print("The error is connect_mssql_connect: ",e)
        return dummy_conn ,connect_flag

def connect_append_postgresql(df):
    try:
        connection = psycopg2.connect(
            user=os.getenv('PG_DB_USERNAME'),
            password=os.getenv('PG_DB_PASSWORD'),
            host=os.getenv('PG_DB_SERVER'),
            port=os.getenv('PG_DB_PORT'),
            database=os.getenv('PG_DB_DATABASE')
        )
        cursor = connection.cursor()
        
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL:", error)
    
    try:
        values = df.values.tolist()
        table_name = os.getenv('PG_DB_TABLE')
        columns = ', '.join(df.columns)
        
        for value in values:
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s']*len(value))})"
            cursor.execute(sql, value)

        connection.commit()
        print("Data appended successfully")
    except (Exception, Error) as error:
        connection.rollback()
        print("Error while appending data to the table:", error)
    
    cursor.close()
    connection.close()

def take_last_append_date_postgresql():
    try:
        connection = psycopg2.connect(
            user=os.getenv('PG_DB_USERNAME'),
            password=os.getenv('PG_DB_PASSWORD'),
            host=os.getenv('PG_DB_SERVER'),
            port=os.getenv('PG_DB_PORT'),
            database=os.getenv('PG_DB_DATABASE')
        )
        cursor = connection.cursor()
        
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL:", error)
    
    try:
    
        cursor.execute("""SELECT yabby_kod, MAX(datalogged) AS max_datalogged
                            FROM mind4machine.textracklocationcheck
                            GROUP BY yabby_kod
                            ORDER BY max_datalogged DESC;""")
        temp = cursor.fetchall()
        df = pd.DataFrame(temp)
        mapping = {df.columns[0]:'yabby_kod', df.columns[1]:'datalogged'}
        datalogged = df.rename(columns=mapping)
        datalogged['datalogged'] = pd.to_datetime(datalogged['datalogged']) + pd.Timedelta(seconds=1)
        return datalogged
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    
    cursor.close()
    connection.close()

def take_last_end_date_postgresql():
    try:
        connection = psycopg2.connect(
            user=os.getenv('PG_DB_USERNAME'),
            password=os.getenv('PG_DB_PASSWORD'),
            host=os.getenv('PG_DB_SERVER'),
            port=os.getenv('PG_DB_PORT'),
            database=os.getenv('PG_DB_DATABASE')
        )
        cursor = connection.cursor()
        
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL:", error)
    
    try:
    
        cursor.execute("""SELECT yabby_kod, MAX(datalogged) AS max_datalogged
                            FROM mind4machine.textracklocationcheck
                            where enddate is not null
                            GROUP BY yabby_kod
                            ORDER BY max_datalogged DESC;""")
        temp = cursor.fetchall()
        df = pd.DataFrame(temp)
        mapping = {df.columns[0]:'yabby_kod', df.columns[1]:'datalogged'}
        datalogged = df.rename(columns=mapping)
        datalogged['datalogged'] = pd.to_datetime(datalogged['datalogged']) + pd.Timedelta(seconds=1)
        return datalogged
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    
    cursor.close()
    connection.close()