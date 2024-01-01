# Overview
The project is entirely built in Python. When a file arrives in a specified directory, the process will pick it up, transform and save it in the destination directory.

## Scripts
Here is the structure of the project.

![image](https://github.com/hanbie123/hb/assets/155374550/757d4787-6392-4836-8c76-d74110e7c00f)


- config
  - config.py - contains variables (eg which fields should be anonymised), paths will need to be set in this file
  - configure_log.py - configuration of the logging process
- ingestion
  - read_write.py - functions to read, write or append json.gz files
- transformations
  - erasure_functions.py - functions specific for the erasure request process
  - process_files.py - contains all logic; reads data, transforms data, logs and saves data
  - transformations.py - functions for transforming customers, products and trasformations datasets
  - utils.py - generic help functions
- main.py - calls functions that process files (from process_files.py)
- monitor.py - this script will watch directory, and run main on arrival of the file, it will need to continuosly run

## Process overview
### Customers
1. json.gz file arrives
2. Rows with missing values in required fields are removed
3. Rows with ids that already existed are removed
4. Writes cleaned data to new directory under the same date in format json.gz
5. Appends master file with all customer ids, emails, and output file locations (used by erasure process)
6. Saves quarantined rows
7. Logs all transformations

### Products
1. json.gz file arrives
2. Rows with missing values in required fields are removed
3. Rows with ids that already existed are removed
4. Rows with values not greater than 0 in specified columns are removed
5. Writes cleaned data to new directory under the same date in format json.gz
6. Appends master file with all sku numbers and output file locations
7. Saves quarantined rows
8. Logs all transformations

### Transactions
1. json.gz file arrives
2. Rows with missing values in required fields are removed
3. Rows with ids that already existed are removed
4. Rows containt non existing customer id or sku product number are removed
6. Writes cleaned data to new directory under the same date in format json.gz
7. Appends master file with all transaction ids and output file locations
8. Saves quarantined rows
9. Logs all transformations
    
### Erasure
1. json.gz file arrives
2. for each requests
    1. finds file location in customer master file
    2. hashes the customer information in the found location
3. Hashes requests in the master file
4. Hashes requests in quarantine file
5. Hashes requests in erasure-requests file and saves it
6. Logs all steps

# Running
Below is the instruction how to test the code.
## Steps
- Clone repo to your local directory
- Set up virtual environment (if you wish to use it)
- Install requirements.py
- Update config.py file (see below)
- Run monitor.py
- Drop files in the monitored directory (specified in the config.py)

## Update config.py
Update the following variables in the config.py
- input_root - path to the source data, this path will be monitored by monitor.py
- output_root - specify where transformed data should be saved
- log_file_path - location for the log file (please note; it's a path directly the file with extention .log)
- map_paths - paths for the master files (containing unique ids) - the files should have extention .json.gz
- quarantine_paths - paths for the quarantine files - the files should have extention .json.gz
  

![image](https://github.com/hanbie123/hb/assets/155374550/14550821-4669-4457-aca3-8af88171e860)

