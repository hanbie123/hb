import pathlib
import config.config as cnf

def get_file_name(file_path:str) -> str:
    """Returns name of the file without suffixes
    from the given path
    Args:
        file_path(str): path to a file
    Returns:
        file name
    """
    file_path = pathlib.Path(file_path)
    file_name = file_path.name
    # remove suffixes
    file_stem = file_name.split(".")[0]
    return file_stem

def get_output_path(input_path:str) -> str:
    """Replaces root of the path
    Args:
        input_path(str): input path
    Returns:
        path with updated root
    """

    input_path = pathlib.Path(input_path)
    input_root = pathlib.Path(cnf.input_root)
    output_root = pathlib.Path(cnf.output_root)

    # change to absolute paths
    input_path = input_path.resolve()
    input_root = input_root.resolve()
    output_root = output_root.resolve()

    # check if input_path is within input_root
    try:
        relative_path = input_path.relative_to(input_root)
        return str(output_root / relative_path)
    except ValueError:
        raise ValueError("input path is not inside input root")