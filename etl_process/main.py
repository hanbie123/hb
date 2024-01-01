from transformation.process_files import process_file
import sys
import pathlib
import config.config as cnf
from config.configure_log import configure_log

if __name__ == "__main__":
    
    configure_log(cnf.log_file_path)

    file_path_str = sys.argv[1]
    file_path = pathlib.Path(file_path_str)

    if file_path.is_file():
        print('Processing:', file_path)
        process_file(file_path_str)
        print('File processed')

