import csv
import numpy

SOUTH_BORDER_LAT = 24.08333333
NORTH_BORDER_LAT = 49.91666667
WEST_BORDER_LON = -125.000
EAST_BORDER_LON = -66.500
CELL_WIDTH = 4166666/100000000

with open('results/prism-grid.csv', 'w', newline='') as csvfile:
    gridwriter = csv.writer(csvfile)
    cell_id = 1
    gridwriter.writerow(['cell_id', 'lat', 'lon'])
    for lat in numpy.arange(SOUTH_BORDER_LAT, NORTH_BORDER_LAT, CELL_WIDTH): 
        for lon in numpy.arange(WEST_BORDER_LON, EAST_BORDER_LON, CELL_WIDTH):
            gridwriter.writerow([cell_id, round(lat, 3), round(lon, 3)])
            cell_id = cell_id + 1    
