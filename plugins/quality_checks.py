from pathlib import Path
import pandas as pd
import logging


def check_if_file_exists(filepath: str, additional_message: str = '') -> None:
    if not Path(filepath).exists():
        raise ValueError(f'The file\n\t"{filepath}"\ndoes not exist\n{additional_message}')


def check_nullability(
        parquet_filepath: Path,
        critical_null_percentage: float,
        warning_null_percentage: float,
) -> None:
    """
    This function:
        1) reads in the parquet_filepath
        2) calculates the percentages of null values for each column
        3) checks which (if any) columns have more than critica_null_percentage null values
        4)  checks which (if any) columns have more than warning_null_percentage null values
    """
    df = pd.read_parquet(parquet_filepath)
    null_percentages_per_column = 100 * df.isnull().mean().round(2)
    above_critical = (null_percentages_per_column > critical_null_percentage)
    if any(above_critical):
        error_msg = (
            f'The following columns had more than {critical_null_percentage}% values missing:\n'
            f'{df.columns[above_critical == True].tolist()}'
        )
        raise ValueError(error_msg)

    above_warning = (null_percentages_per_column > warning_null_percentage)
    if any(above_warning):
        warning_msg = (
            f'The following columns had more than {warning_null_percentage}% values missing:\n'
            f'{df.columns[above_warning == True].tolist()}'
        )
        logging.warning(warning_msg)
