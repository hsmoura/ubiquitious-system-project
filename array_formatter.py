import csv

if __name__ == '__main__':

	with open('processed_trips_formatted.csv', 'w') as converted_file: 
		writer = csv.writer(converted_file, delimiter=';')
		with open('processed_trips.csv', 'r') as origin_file:
			reader = csv.reader(origin_file, delimiter=';')
			# skip header			
			next(reader)
			for row in reader:
				row[2] = str(row[2]).replace('[', '{')
				row[2] = str(row[2]).replace(']', '}')
				writer.writerow(row)
		origin_file.close()
	converted_file.close()
