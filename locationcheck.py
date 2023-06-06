from shapely.geometry import Point, Polygon
import configparser
import ast
import database as database
import pandas as pd

config = configparser.ConfigParser()
config.read('config.ini')

coordinate =[]
zone_check = []
purpleCoords = ast.literal_eval(config['DEFAULT']['purpleCoords'])
yellowCoords = ast.literal_eval(config['DEFAULT']['yellowCoords'])
redCoords = ast.literal_eval(config['DEFAULT']['redCoords'])
blueCoords = ast.literal_eval(config['DEFAULT']['blueCoords'])

def point_inside_polygon(point, purpleCoords, yellowCoords, redCoords, blueCoords):
        point = Point(point[0], point[1])

        purplepolygon = Polygon(purpleCoords)
        yellowpolygon = Polygon(yellowCoords)
        redpolygon = Polygon(redCoords)
        bluepolygon = Polygon(blueCoords)

        if purplepolygon.contains(point):
            return 1 # 'Mor kisimda.'
        elif yellowpolygon.contains(point):
            return 2 # 'Sari kisimda.'
        elif redpolygon.contains(point):
            return 3 #'Kirmizi kisimda.'
        elif bluepolygon.contains(point):
            return 4 #'Mavi kisimda.'
        else:
            return -1
def main(merge_apidata,flag):
    global zone_check
    df = merge_apidata.copy(deep=True)
    df = df.drop_duplicates(subset=['DataLogged','PARKUMNO'], keep='first')
    for index, row in df.iterrows():
        latitude = row['Latitude']
        longitude = row['Longitude']
        coordinate =(latitude , longitude)
        result = point_inside_polygon(coordinate, purpleCoords, yellowCoords, redCoords, blueCoords)
        zone_check.append(result)
    df['zone_check'] = zone_check
    zone_check = []
    df.to_csv('zone_check_result.csv', index=False)
    database.connect_append_postgresql(df)
    if flag == 'start':
        flag = 'end'
        last_append_date = database.take_last_end_date_postgresql
        return last_append_date, flag
    elif flag == 'end':
        flag = 'start'
        last_append_date = database.take_last_append_date_postgresql()
        return last_append_date, flag