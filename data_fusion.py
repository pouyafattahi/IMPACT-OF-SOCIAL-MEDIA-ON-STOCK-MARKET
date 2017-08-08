import os, tarfile, glob, sys, zipfile

data_2016 = sys.argv[1] + '/2016'
data_2017 = sys.argv[1] + '/2017'
sectors_path = sys.argv[1] + '/sectors'


def duplicate_removal(input_file, output_file):
	with open(input_file,'r') as in_file, open(output_file,'w') as out_file:
		seen = set()
		for line in in_file:
			if line in seen: continue # skip duplicate

			seen.add(line)
			out_file.write(line)

	return output_file

#This function combines yearly data into a single file
def combine_yearly_data(input_path, output_csv, output_txt):
	out_prices = open(output_csv, "a")
	out_symbols = open(output_txt, "a")
	header_saved = False
	for tar_file in glob.glob(input_path+ "/*.gz"):
		tar = tarfile.open(tar_file,"r")
		for member in tar.getmembers():
			f = tar.extractfile(member)
			if member.name.find("prices") >=0:
				for line in f.read():
					out_prices.write(line)
			
			elif member.name.find("symbols") >=0:
				for line in f.read():
					out_symbols.write(line)
	out_prices.close()
	out_symbols.close()
	return out_prices, out_symbols

#This function combines all the yearly prices csv file into a single file 
def combine_final_prices(input_files, output_file):
	with open("temp.csv",'wb') as fout:
	    for filename in input_files:
	        with open(filename) as fin:
	            for line in fin:
	                fout.write(line)
	fout.close()

	out_path = duplicate_removal("temp.csv", output_file)
	os.remove("temp.csv")
	return out_path

#This function combines the multiple symbols text file into a single file
def combine_final_symbols(input_files, output_file):
	with open("temp.txt", "wb") as outfile:
	    for f in input_files:
	        with open(f, "rb") as infile:
	            outfile.write(infile.read())	
	outfile.close()
	
	out_path = duplicate_removal("temp.txt", output_file)
	os.remove("temp.txt")
	return out_path

def combine_sector_data(path, output_file):
	input_files = glob.glob(path+"/*.csv")
	with open("temp.csv",'wb') as fout:
	    for filename in input_files:
	        with open(filename) as fin:
	            for line in fin:
	                fout.write(line)
	fout.close()
	
	out_path = duplicate_removal("temp.csv", output_file)
	os.remove("temp.csv")
	return out_path

def get_zip(prices, symbols, sectors):
	zip_name = "stock_input.zip"
	zip_files = zipfile.ZipFile(zip_name, 'w')
	zip_files.write(prices)
	zip_files.write(symbols)
	zip_files.write(sectors)
	zip_files.close()
	os.remove(prices)
	os.remove(symbols)
	os.remove(sectors)
	return zip_files


def get_fusion_files(data_2016, data_2017, sectors_path):
	#Combining files yearly
	prices_2016, symbols_2016 = combine_yearly_data(data_2016, "2016_prices.csv", "2016_symbols.txt")
	prices_2017, symbols_2017 = combine_yearly_data(data_2017, "2017_prices.csv", "2017_symbols.txt")

	#Listing out the files to be combined into one
	prices_files = ["2016_prices.csv", "2017_prices.csv"]
	symbols_files = ["2016_symbols.txt", "2017_symbols.txt"]

	#Final prices and symbols file and get the path to these files
	prices = combine_final_prices(prices_files, "prices_final.csv")
	symbols = combine_final_symbols(symbols_files, "symbols_final.txt")

	sectors = combine_sector_data(sectors_path, "sectors_final.csv")

	os.remove("2016_prices.csv")
	os.remove("2017_prices.csv")
	os.remove("2016_symbols.txt")
	os.remove("2017_symbols.txt")

	zip_files = get_zip(prices, symbols, sectors)
	return zip_files

stock_zip = get_fusion_files(data_2016, data_2017, sectors_path)



