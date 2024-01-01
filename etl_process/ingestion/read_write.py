import pathlib
import gzip
import pandas as pd

def read_gzip_json(file_path:str) -> pd.DataFrame:
    """Opens data in json.gz or .json format,
    and loads it to pandas dataframe
    Args:
        file_path(str): path to the file
    Returns:
        Pandas dataframe with data from the file
    """

    file_path = pathlib.Path(file_path)

    if '.gz' in file_path.suffixes:
        with gzip.open(file_path, 'rt', encoding='UTF-8') as zipfile:
            return pd.read_json(zipfile, lines=True)
    else:
        with open(file_path, 'rt', encoding="UTF-8") as file:
            return pd.read_json(file, lines=True)

def write_gzip_json(output_path, data):
    """Writes data to json.gz file
    Args:
        output_path(str): path where to save the file
        data(pd.DataFrame): pandas dataframe with data to save
    """
    output_path = pathlib.Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if '.gz' in output_path.suffixes:
        with gzip.open(output_path, 'wt') as f:
            data.to_json(f, orient='records', lines=True, index=False)
    else:
        with open(output_path, 'wt') as f:
            data.to_json(f, orient='records', lines=True, index=False)

def append_gzip_json(output_path, data):
    """Appends json.gz file
    Args:
        output_path(str): path where to save the file
        data(pd.DataFrame): pandas dataframe with data to append
    """
    output_path = pathlib.Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(output_path, 'a') as f:
        data.to_json(f, orient='records', lines=True, index=False)