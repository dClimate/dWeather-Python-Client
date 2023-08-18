import os
import re
import zipfile
import pdb
import sys
from datetime import datetime

# input zip file name
input_zip_file = 'cme_futures_hist.zip'

# create an empty hash list
# this hash list will eventually contain data like 'D0':10, 'H1':20
# which means that we read 10 data lines for the station 'D0' and 20 data lines for the station 'H1'
# station_hash = {
#    'D0': 10,
#    'H1': 20
# }
station_hash = {}

# the output folder where we are going to store data files by station
output_folder = 'cme_futures_hist_by_station'

# if the output folder already exists, remove it
if (os.path.exists(output_folder)):
	print(output_folder + ' exists! Please delete the folder and try again.')
	sys.exit()

os.mkdir(output_folder)

# open the input zip file
with zipfile.ZipFile(input_zip_file, 'r') as input_zip_ref:

	# get a list of all the file names in the input zip file
	# there will be one file per folder (cme_futures_hist/2022-07-13/K5K)
	# there will be one file per csv file (cme_futures_hist/2022-07-13/K5K/data.csv)
	# there will be one file per metadata.json file (cme_futures_hist/2022-07-13/metadata.json)
	file_list = input_zip_ref.namelist()

	# iterate over the file_list, get the name of each file
	for file_name in file_list:

		# if the file is not a data.csv file, then ignore the file
		# for example, ignore folders and ignore metadata.json files
		if (not re.search(r'data.csv', file_name)):
			continue

		# print the csv file (if needed)
		# for example, cme_futures_hist/2022-07-13/K5K/data.csv  
		# print(file_name)

		# extract the station name from the file name
		# the split function will split the file name by '/' into an array
		# index 2 in the returned array will contain the station name
		# for example, in cme_futures_hist/2022-07-13/K5K/data.csv, KSV is at index 2
		subfolders_list = file_name.split('/')
		dt = subfolders_list[1]
		station_name = subfolders_list[2]

		# print the station name (if needed)
		# print(station_name)

		#if (station_name != 'HS'):
			#continue

		# store this file_name as input_csv_file (for clarity's sake)
		input_csv_file = file_name

		#pdb.set_trace()

		# now open in the input csv file (cme_futures_hist/2022-07-13/K5K/data.csv)
		with input_zip_ref.open(input_csv_file) as input_csv_file_ref:
			# read the input csv file, the data will be read as binary data
			binary_data = input_csv_file_ref.read()

			# convert the binary data to ascii (text) data
			ascii_data = binary_data.decode('ascii')

			# split ascii_data by "\n" and assign back to ascii_data
			# this will convert ascii_data to an array
			ascii_data = ascii_data.split("\n")

			# delete the first line in ascii_data (dt,value)
			del ascii_data[0]

			# create an empty array (input_lines)
			# add to input_lines only those lines from ascii date that have any date (ignore blank lines)
			input_lines = []
			for line in ascii_data:
				if (len(line) > 0):
					parts = line.split(',')
					forecasted_dt = datetime.strptime(parts[0], '%Y%m').strftime('%Y-%m')
					csv_line = forecasted_dt + ',' + parts[1] + ',' + dt + "\n"
					input_lines.append(csv_line)

			# note how many lines of data.csv were read for this station ('KSK')
			count = len(input_lines)

			# if station_name ('K5K') is being encountered for the first time
			# note that we have processed zero lines for this station as of yet
			if (not station_name in station_hash):
				station_hash[station_name] = 0

			# now, make a note of many lines of input csv file (data.csv) that we processed for 'K5K'
			# ignore the column header
			station_hash[station_name] = station_hash[station_name] + count
			
				
		# now, we will create single data file (K5K/data.csv) for the station 'K5K'
		# we will store this file in the output folder (./cme_futures_hist_by_station)
		#pdb.set_trace()
        
		station_folder = os.path.join(output_folder, station_name)
		output_csv_file = os.path.join(station_folder, 'data.csv')

		if (not os.path.exists(output_csv_file)):
			# if this is the first time for 'K5K/data.csv', create a subfolder for 'K5K' under the output folder (cme_futures_hist_by_station)
			# then, create the file
			#print('creating folder', station_folder)
			os.mkdir(station_folder)
			input_lines = ["forecasted_dt,SETT,dt\n"] + input_lines

		# now, append the input_lines to the output_csv_file (K5K/data.csv)
		with open(output_csv_file, 'a', encoding='ascii') as output_csv_file_ref:
			output_csv_file_ref.writelines(input_lines)


# output zip file name
output_zip_file = 'cme_futures_hist_by_station.zip'

with zipfile.ZipFile(output_zip_file, 'w', zipfile.ZIP_DEFLATED) as output_zip_file_ref:
	# get the root, dirs, and files in the output folder
	for root, dirs, files in os.walk(output_folder):
		for file in files:
			#pdb.set_trace()

			# construct the file path file for each file
			# root ='cme_futures_hist_by_station/D0'  
			# file = 'data.csv'
			data_csv_file = os.path.join(root, file)

			with open(data_csv_file, 'r', encoding='ascii') as data_csv_file_ref:
				lines = data_csv_file_ref.readlines()

			temp_array = []
			for line in lines:
				line = line.rstrip("\n")
				parts = line.split(',')
				temp_array.append(parts)

			sorted_array = sorted(temp_array, key=lambda x: x[2], reverse=True)
			with open(data_csv_file, 'w', encoding='ascii') as data_csv_file_ref:
				for parts in sorted_array:
					line = ','.join(parts)
					line += "\n"
					data_csv_file_ref.write(line)

			# write the file to the zip file
			output_zip_file_ref.write(data_csv_file, os.path.relpath(data_csv_file, output_folder))
			