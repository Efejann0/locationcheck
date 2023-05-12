from shapely.geometry import Point, Polygon
from geopy.distance import geodesic

def point_inside_polygon(point, polygon_coords):
    # Create a shapely Point object from the given coordinates
    point = Point(point[0], point[1])

    # Create a shapely Polygon object from the given list of coordinates
    polygon = Polygon(polygon_coords)

    # Check if the point is within the polygon
    return point.within(polygon)

# Define your point and polygon coordinates
point = (38.618190, 27.365351)  # Example point (longitude, latitude)
polygon_coords = [
    (38.618598, 27.365637),
    (38.618619, 27.365820),
    (38.618482, 27.365860),
    (38.618469, 27.365703)
]  # Example polygon (list of longitude, latitude tuples)

result = point_inside_polygon(point, polygon_coords)

print("Is the point inside the polygon?", result)
