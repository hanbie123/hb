import pandas as pd
import hashlib

def get_dict(filter_list:list, field_name:str, df_map:pd.DataFrame) -> dict:
    """For a given list of ids/emails returns a dict
    with file locations for each of them,
    {cust_id1: location1,
    cust_id2: location2, ...}
    Args:
        filter_list(list): values to filter
        field_name(str): name of the id to filter by
        df_map(pd.DataFrame): dataframe with customers ids/emails
            and their file locations
    Returns:
        A dictionary with file locations for each id/email
    """
    # filter by field_name
    df = df_map[df_map[field_name].isin(filter_list)]
    # convert to dict
    loc_dict = df[[field_name, 'source_location']].set_index(field_name).T.to_dict()
    return loc_dict

def get_locations(df_erasure:pd.DataFrame, df_map:pd.DataFrame, df_quarantine:pd.DataFrame) -> dict:
    """For a given erasure request returns a dict
    with file locations for each customer,
    {'id': {cust_id1: location1, cust_id2: location2, ...},
    'email': {email1: location1, email2: location2, ...}}
    where key 'email' contains requests that contained only email address
    Args:
        df_erasure(pd.DataFrame): dataframe with erasure request
        df_map(pd.DataFrame): dataframe with aggregated customers ids and emails,
            and location of the file where they are saved
        df_quarantine(pd.DataFrame): dataframe with quarantined rows
    Returns:
        A dictionary with file locations for each id/email
    """
    # requests only with a given email
    df_email = df_erasure[df_erasure['customer-id'].isna()]
    emails = df_email['email'].to_list()

    # requests only with a given customer id
    df_customer = df_erasure[df_erasure['email'].isna()]
    ids = df_customer['customer-id'].to_list()

    # requests with customer id and email populated
    df_both = df_erasure[~df_erasure['customer-id'].isna() & ~df_erasure['email'].isna()]

    # filter for the records where both id and email match
    df_map = pd.concat([df_map, df_quarantine])
    merged_df = pd.merge(df_map, df_both,
                        left_on=['id','email'],
                        right_on=['customer-id','email'],
                        how='inner')
    ids_both = merged_df['customer-id'].to_list()

    # combine lists with customee ids
    ids.extend(ids_both)

    # save all in dict
    dict_loc = {}
    dict_loc['id'] = get_dict(ids, 'id', df_map)
    dict_loc['email'] = get_dict(emails, 'email', df_map)

    return dict_loc

def hash_data(data:str):
    """Hashes given data
    Args:
        data(str): data to hash
    Returns:
        hex representation of the hash
    """
    # data to bytes
    if not isinstance(data, bytes):
        data = str(data).encode()

    # reate new hash object
    hash_object = hashlib.sha256()
    hash_object.update(data)

    # return hex representation of the hash
    return hash_object.hexdigest()

def hash_per_id(row:list, id_field_name:str,
                id_to_hash:str, hash_field_name:str):
    """Hashes field only for a given id
    Args:
        row(list): row in a dataframe
        id_field_name(str): name of the field that determines
            if data should be hashed (customer id or email)
        id_to_hash(str): value of id_field_name for each
            data should be hashed
        hash_field_name(str): field to hash
    Returns:
        Hashed value
    """
    if row[id_field_name] == id_to_hash:
        return hash_data(row[hash_field_name])
    else:
        return row[hash_field_name]
    
def hash_df(df:pd.DataFrame, id_field_name:str, id_to_hash:str,
            hash_field_name_list:list):
    """Hashes fields only for a given id
    Args:
        df(pd.DataFrame): dataframe containing data
            that needs to be hashed
        id_field_name(str): name of the field that determines
            if data should be hashed (customer id or email)
        id_to_hash(str): value of id_field_name for each
            data should be hashed
        hash_field_name_list(str): list fields to hash
    Returns:
        Dataframe with hashed values
    """
    for hash_field_name in hash_field_name_list:
        df[hash_field_name] = df.apply(lambda row: hash_per_id(row, id_field_name, id_to_hash, hash_field_name), axis=1)
    return df

def missed_requests(df_erasure:pd.DataFrame, dict_loc:dict):
    """Some requests could have been missed,
    for example if email wasn't consistent with id,
    or it was repetitive request.
    Finds missed request and prints a 'warning'
    Args:
        df_erasure(pd.DataFrame): data with requests
        dict_loc(dict):dict with all request that will be processed
    """
    ids = df_erasure['customer-id'].to_list()

    df_email = df_erasure[df_erasure['customer-id'].isna()]
    emails = df_email['email'].to_list()

    missed_ids = [id for id in ids if id not in dict_loc['id'].keys()]
    missed_emails = [email for email in emails if email not in dict_loc['email'].keys()]
    if missed_ids or missed_emails:
        df_erasure = df_erasure[df_erasure['customer-id'].isin(missed_ids) | df_erasure['email'].isin(missed_emails)]
        return f'Following erasure requests were not processed: {df_erasure.values.tolist()}'
    return False
