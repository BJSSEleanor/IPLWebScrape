"""Load functions to save the transformed_data csv to a database.

Functions:
    load_csv(str) -> DataFrame
    load(str) -> None

Classes:
    Batters(Base)
"""
from sqlalchemy import create_engine
import pandas as pd


def load_csv(file_name: str) -> pd.DataFrame:
    """Loads a csv and returns the data and headers.

    Args:
        file_name (str): The file path for the csv.

    Returns:
        DataFrame: A dataframe of the file records.
    """
    data: pd.DataFrame = pd.read_csv(file_name)
    return data


def load(file_name: str):
    """Loads the csv into a local sqlite db."

    Args:
        file_name (str): The location of the csv.
    """
    data = load_csv(file_name)
    print(data)
    engine = create_engine("sqlite:///data/ipl_2022.db")
    with engine.connect() as connection:
        data.to_sql(
            name="batters",
            con=engine,
            if_exists="replace",
            method="multi",
            index=True,
            index_label="PlayerID")