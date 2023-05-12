from shapely.geometry import Point, Polygon
import configparser
import ast
import pandas as pd

config = configparser.ConfigParser()
config.read('config.ini')

coordinate =[]
inside_outside = []
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
            return 'Mor kisimda.'
        elif yellowpolygon.contains(point):
            return 'Sari kisimda.'
        elif redpolygon.contains(point):
            return 'Kirmizi kisimda.'
        elif bluepolygon.contains(point):
            return 'Mavi kisimda.'
        else:
            return 'Belirlenen bolgelerde degil.'
def main():
    df = pd.read_csv('data.csv')
    for index, row in df.iterrows():
        latitude = row['Latitude']
        longitude = row['Longitude']
        coordinate =(latitude , longitude)
        print(f"Satır {index}: Kolon1 = {latitude}, Kolon2 = {longitude}")
        result = point_inside_polygon(coordinate, purpleCoords, yellowCoords, redCoords, blueCoords)
        inside_outside.append(result)
        print("Is the point inside the polygon?", result)
    df['inside_outside'] = inside_outside
    df.to_csv('inside_outside_result.csv', index=False)


# if __name__ == '__main__':
#     main()
