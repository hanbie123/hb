import pandas as pd
from ingestion.read_write import read_gzip_json, write_gzip_json, append_gzip_json
import transformation.transformations as t
import config.config as cnf
from transformation.utils import get_file_name, get_output_path
import pathlib
import transformation.erasure_functions as ef
import logging

def append_ids(input_path:str, data:pd.DataFrame, quarantine:bool=False):
    """Appends master file with all ids,
    and locations of the files in which they appeared
    Args:
        input_path(str): path of the file thta just arrived
        data(pd.DataFrame): data from the file that just arrived
        quarantine(bool): if appending quarantine file (True) or master file (False)
    """
    # get name of the file from path, eg customers
    file_name = get_file_name(input_path)

    # get path to the master file with ids and files locations
    if quarantine:
        map_path = pathlib.Path(cnf.quarantine_paths[file_name])
    else:
        map_path = pathlib.Path(cnf.map_paths[file_name])

    # create df with eg customer_id, email, file_location for the current file
    output_path = get_output_path(input_path)
    if quarantine:
        data = t.source_location_map(data, source=output_path)
    else:
        data = t.source_location_map(data, cnf.map_columns[file_name], output_path)

    # append the master file
    if map_path.is_file():
        append_gzip_json(map_path, data)
    else:
        write_gzip_json(map_path, data)

def process_data(file_name:str, input_path:str, output_path:str=None):
    """Process customers, products or transformations files,
    and saved them in different location
    Args:
        file_name(str): file name that arrived
        input_path(str): path of the file thta just arrived
        output__path(str): path where transformed file should be saved
    """
    try:
        if output_path is None:
            output_path = get_output_path(input_path)

        # read data into pandas df
        df = read_gzip_json(input_path)
        row_count = len(df)
        logging.info(f"Processing {input_path}, read in {row_count} rows.")

        # remove rows where fields that should be populated are not
        df, df_quarantine = t.remove_rows_with_na(df, cnf.not_empty_cols[file_name])
        row_count_empty = len(df)
        if row_count - row_count_empty:
            logging.info(f"{row_count - row_count_empty} rows were missing data in {cnf.not_empty_cols[file_name]}.")

        # remove rows with ids that already existed
        df = t.remove_id_dups(df, cnf.map_paths[file_name], cnf.unique_col[file_name])
        row_count_id_dups = len(df)
        if row_count_empty - row_count_id_dups:
            logging.info(f"{row_count_empty - row_count_id_dups} rows contained non unique id.")

        # remove rows with negative values
        if file_name == 'products':
            df = t.keep_positive_vals(df, cnf.positive_col[file_name])
            row_count_pos = len(df)
            if row_count_id_dups - row_count_pos:
                logging.info(f"{row_count_id_dups - row_count_pos} rows removed as contained negative values in {cnf.positive_col[file_name]}.")

        # remove transactions where total cost doesn't match
        if file_name == 'transactions':

            # remove rows where total cost is wrong
            df, df_q = t.total_cost_check(df)
            row_count_cost = len(df)
            if row_count_id_dups - row_count_cost:
                logging.info(f"{row_count_id_dups - row_count_cost} rows removed as total cost didn't match.")

            df_quarantine = pd.concat([df_quarantine, df_q])

            # check if products or customers arrived at the same time,
            # in this case they could be missing from master file, get them separately

            current_path = pathlib.Path(input_path).parent

            file_path = pathlib.Path(current_path, 'products.json.gz')
            if file_path.is_file():
                df = t.id_exists_check(df, 'sku', 'products', file_path)
        
            file_path = pathlib.Path(current_path, 'customers.json.gz')
            if file_path.is_file():
                df = t.id_exists_check(df, 'customer_id', 'customers', file_path)

            row_count_ids = len(df)
            if row_count_cost - row_count_ids:
                logging.info(f"{row_count_cost - row_count_ids} rows had customer_id and sku that didn't match the rest of the dataset.")

        row_count_final = len(df)
        # append file with ids
        append_ids(input_path, df)
        logging.info(f"{row_count_final} rows appended to master file with ids.")

        # append quarantine file
        if not df_quarantine.empty:
            append_ids(input_path, df_quarantine, quarantine=True)
            logging.info(f"{len(df_quarantine)} rows appended to quarantine file.")

        # save processed file
        write_gzip_json(output_path, df)
        logging.info(f"{row_count_final} rows written to {output_path}.")

    except Exception as e:
        logging.error(f"Error while processing {input_path}: {e}")

def erasure(input_path:str, output_path:str=None):
    """Hashes data of the customer from the erasure-requests file
    Args:
        input_path(str): path of the file thta just arrived
        output__path(str): path where transformed file should be saved
    """
    try:
        if output_path is None:
            output_path = get_output_path(input_path)

        df_erasure = read_gzip_json(input_path)

        # remove rows where both fields are empty
        df_erasure = df_erasure.dropna(how='all')
        row_count = len(df_erasure)
        logging.info(f"Processing {row_count} erasure requests.")

        # load customer id master file
        if pathlib.Path(cnf.map_paths['customers']).is_file():
            map_path = pathlib.Path(cnf.map_paths['customers'])
            customers_df = read_gzip_json(map_path)
        else:
            customers_df = pd.DataFrame()

        # load customer quarantined file
        quarantine_path = pathlib.Path(cnf.quarantine_paths['customers'])
        quarantine_master_df = read_gzip_json(quarantine_path)

        # dict with ids from erasure requests and file locations
        dict_loc = ef.get_locations(df_erasure, customers_df, quarantine_master_df)

        # hash per customer id, and then by email if only email was available
        for col_name in ['id', 'email']:
            for id_email_val, loc in dict_loc[col_name].items():
                # read transformed file
                df = read_gzip_json(loc['source_location'])
        
                # hash data
                df = ef.hash_df(df, col_name, id_email_val, cnf.anonymisation)

                # hash in map file
                customers_df = ef.hash_df(customers_df, col_name, id_email_val, ['email'])

                # hash in quarantine file
                quarantine_master_df = ef.hash_df(quarantine_master_df, col_name, id_email_val, cnf.anonymisation)

                # hash in erasure file
                col_name_erasure = 'customer-id' if col_name == 'id' else col_name
                df_erasure = ef.hash_df(df_erasure, col_name_erasure, id_email_val, ['email'])
        
                # save transformed customers.json.gz with hashed data
                write_gzip_json(loc['source_location'], df)
                logging.info(f"A request hashed in {loc['source_location']}.")

        # save file with all customer ids, emails and file locations, with hashed data
        write_gzip_json(map_path, customers_df)
        logging.info(f"Requests hashed in {map_path}.")

        # save quarantined records with hashed data
        write_gzip_json(quarantine_path, quarantine_master_df)
        logging.info(f"Requests hashed in {quarantine_path}.")

        # save erasure request file, with hashed data
        write_gzip_json(output_path, df_erasure)
        logging.info(f"Requests hashed in {output_path}.")

        # log missed request, eg in case email didn't match id or it was a repetitive request
        missed = ef.missed_requests(df_erasure, dict_loc)
        if missed:
            logging.warning(missed)

    except Exception as e:
        logging.error(f"Error while processing {input_path}: {e}")

def process_file(input_path:str, output_path:str=None):
    """Based on the name of the file that arrived,
    transforms data or does erasure request
    Args:
        input_path(str): path of the file thta just arrived
        output__path(str): path where transformed file should be saved
    """
    if output_path is None:
        output_path = get_output_path(input_path)

    # get file name - 'customers', 'transactions' etc
    file_name = get_file_name(input_path)

    if file_name in ['customers', 'products', 'transactions']:
        process_data(file_name, input_path, output_path)
    elif file_name == 'erasure-requests':
        erasure(input_path, output_path)