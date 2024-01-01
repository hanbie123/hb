# source data will land in this path
input_root = 'C:\\Users\\...\\test-data'

# transformed data will be saved here
output_root = 'C:\\Users\\...\\transformed'

# path to the log file
log_file_path = 'C:\\Users\\...\\log\\process_files.log'

# paths to master files containing all unique ids
map_paths = {
    'customers': 'C:\\Users\\...\\maps\\customers_map.json.gz',
    'products': 'C:\\Users\\...\\maps\\products_map.json.gz',
    'transactions': 'C:\\Users\\...\\maps\\transactions_map.json.gz'
}

# paths to quarantined rows
quarantine_paths = {
    'customers': 'C:\\Users\\...\\quarantine\\customers.json.gz',
    'products': 'C:\\Users\\...\\quarantine\\products.json.gz',
    'transactions': 'C:\\Users\\...\\quarantine\\transactions.json.gz'
}

# columns that should be anonymised
anonymisation = ['first_name', 'last_name', 'email', 'phone_number', 'address']

# columns that should be saved in the master file
map_columns = {
    'customers': ['id', 'email'],
    'products': ['sku'],
    'transactions': ['transaction_id']
}

# columns that have to be populated
not_empty_cols = {
    'customers': ['id', 'first_name', 'last_name', 'email'],
    'products': ['sku', 'name', 'price', 'category', 'popularity'],
    'transactions': ['transaction_id']
}

# columns that have to be unique within the whole data
unique_col = {
    'customers': 'id',
    'products': 'sku',
    'transactions': 'transaction_id'
}

# columns that have to be greater than 0
positive_col = {
    'products': ['price', 'popularity'],
}