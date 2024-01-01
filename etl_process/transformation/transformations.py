import pathlib
from ingestion.read_write import read_gzip_json
import config.config as cnf
import pandas as pd

pd.options.mode.chained_assignment = None

def remove_rows_with_na(df:pd.DataFrame, col_list:list):
    """Removes rows where some data
    in a given column list is not populated
    Args:
        df(pd.Dataframe): data
        col_list(list): columns that has to be populated
    Returns:
        pandas dataframe with removed columns
    """
    for col in col_list:
        df[col] = df[col].replace('', None)

    df_quarantine = df[df[col_list].isna().any(axis=1)]
    df = df.dropna(subset=col_list)
    return df, df_quarantine

def source_location_map(df:pd.DataFrame, col_list:list=None, source:str=''):
    """Creates a dataframe that contains only ids (eg customer_id and email)
    and location of the output file (eg transformed/2020/01/02/customer.json.gz)
    Args:
        df(pd.Dataframe): data
        col_list(list): columns to save
        source(str): location of the output file
    Returns:
        dataframe with map between id and location to the output file
    """
    if col_list != None:
        df = df[col_list]
    df['source_location'] = source
    return df

def keep_unique_ids(source_df:pd.DataFrame, map_df:pd.DataFrame, col_name:str):
    """Checks if ids with the file that arrived are unique within
    the whole dataset, removes rows if not
    Args:
        source_df(pd.DataFrame): dataframe containing data that just arrived
        map_df(pd.DataFrame): dataframe with aggregated ids
        col_name(str): name of the column containing unique id
    Returns:
        dataframe with removed rows where id wasn't unique
    """
    source_df = source_df.drop_duplicates(subset=col_name)
    return source_df[~source_df[col_name].isin(map_df[col_name])]

def remove_id_dups(df:pd.DataFrame, map_path:str, col_name:str):
    """Checks if ids with the file that arrived are unique within
    the whole dataset, removes rows if not
    Args:
        df(pd.DataFrame): dataframe containing data that just arrived
        map_path(str): path to file with aggregated ids
        col_name(str): name of the column containing unique id
    Returns:
        dataframe with removed rows where id wasn't unique
    """
    # check if aggregated file exists and compare ids
    map_path = pathlib.Path(map_path)
    if map_path.is_file():
        map_df = read_gzip_json(map_path)
        df = keep_unique_ids(df, map_df, col_name)
    return df

def get_ids(file_path:str, col_name:str):
    """Returns list of all ids in a file
    Args:
        file_path(str): path to the file
        col_name(str): name of the column with id
    Returns:
        List of all ids
    """
    df = read_gzip_json(file_path)
    return df[col_name].tolist()
    
def get_sku_ids(purchases:dict):
    """From a given dict with purchases,
    returns list of all product sku numbers
    Args:
        purchases(dict): dict with all purchases for a transaction
    Returns:
        list of all products
    """
    sku_list = []
    for product in purchases['products']:
        sku_list.append(product['sku'])
    return sku_list

def id_exists_check(source_df:pd.DataFrame, source_col:str,
                    file_name:str, file_path:str):
    """Checks if customer ids and sku numbers in a transactions file
    exists within the whole dataset. Removes rows from trasactions
    if customer id or sku number wasn't found.
    Args:
        source_df(pd.DataFrame): dataframe containing transactions
            that just arrived
        source_col(str): name column containing customer id or sku
            in transactions data
        file_name(str): customers or products
        file_path(str): path to customer or products file that arrived
            at the same time as transactions
    Returns:
        Dataframe with the rows removed
    """
    # name of the id for either customers or products dataset
    id_name = cnf.unique_col[file_name]

    # ids that arrived at the same time and may be missing from the master file
    additional_ids = get_ids(file_path, id_name)

    # ids from the master file
    map_path = pathlib.Path(cnf.map_paths[file_name])
    map_df = read_gzip_json(map_path)
    existing_ids = map_df[id_name].to_list()

    # combine lists with ids
    existing_ids.extend(additional_ids)

    # remove rows that contain unknown customer id or sku number
    if source_col == 'sku':
        source_df['sku_ids'] = source_df['purchases'].apply(lambda x: get_sku_ids(x))
        source_df['check'] = source_df['sku_ids'].apply(lambda x: all(sku in existing_ids for sku in x))
        source_df = source_df[source_df['check']]
        source_df = source_df.drop(['check', 'sku_ids'], axis=1)  
    elif source_col == 'customer_id':
        source_df = source_df[source_df[source_col].isin(existing_ids)]

    return source_df

def keep_positive_vals(df:pd.DataFrame, col_names:list):
    """Remove rows where values in the specified columns
    are 0 or less
    Args:
        df(pd.Dataframe): data that just arrived
        col_names(list): names of the columns
            that should contain positive values
    Returns:
        dataframe with removed rows
    """
    for col in col_names:
        df = df[df[col] > 0]
    return df

def get_total_cost(purchases:dict):
    """Calculates total cost of the purchases
    Args:
        purchases(dict): dictionary with purchases
    Returns:
        calculated total cost
    """
    total_cost = 0
    for product in purchases['products']:
        total = float(product['total'])
        total_cost += total
    return round(total_cost, 2)

def total_cost_check(df:pd.DataFrame):
    """Removes rows where total cost doesn't match
    calculated total cost
    Args:
        df(pd.Dataframe): transactions data
    Returns:
        dataframe with transactions where total cost matches
        calculated total
    """
    # calculated total cost
    df['total_calculated'] = df['purchases'].apply(lambda x: get_total_cost(x))

    # get given total cost from the dict
    df['total_cost'] = df['purchases'].apply(lambda x: float(x['total_cost']))

    # compare calculated total cost with the one in the data
    df = df[df['total_calculated']==df['total_cost']]
    df_q = df[df['total_calculated']!=df['total_cost']]

    # rmeove not needed columns
    df = df.drop(['total_calculated', 'total_cost'], axis=1)
    df_q = df_q.drop(['total_calculated', 'total_cost'], axis=1)

    return df, df_q