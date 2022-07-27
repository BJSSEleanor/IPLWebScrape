"""Cleans a batter dataframe and groups it by batter.
Loads and saves the dataframe from/to a csv in the data folder.

Functions:
    drop_columns(Dataframe) -> Dataframe
    rename_columns(Dataframe) -> Dataframe
    clean_rows(Dataframe) -> Dataframe
    clean_sr(Dataframe) -> Dataframe
    clean_player(Dataframe) -> Dataframe
    add_not_out_column(Dataframe) -> Dataframe
    clarify_types(Dataframe) -> Dataframe
    clean(Dataframe) -> Dataframe
    group(Dataframe) -> Dataframe
    transform(Dataframe) -> Dataframe
    obtain_and_transform(None) -> None

"""
import numpy as np
import pandas as pd


def drop_columns(data: pd.DataFrame) -> pd.DataFrame:
    """Removes unwanted columns from the dataframe.

    Args:
        data (DataFrame): A dataframe of batters.

    Returns:
        DataFrame: A dataframe of batters with unwanted columns removed.
    """
    data = data.drop(columns=["Unnamed: 8", "Unnamed: 9"])
    return data


def rename_columns(data: pd.DataFrame) -> pd.DataFrame:
    """Renames columns from the dataframe.

    Args:
        data (DataFrame): A dataframe of batters.

    Returns:
        DataFrame: A dataframe of batters with columns renamed.
    """
    data = data.rename(columns={"BATTING": "Player", "Unnamed: 1": "Status"})
    return data


def clean_rows(data: pd.DataFrame) -> pd.DataFrame:
    """Cleans the rows of the Dataframe, removing unwanted rows.

    Args:
        data (DataFrame): A dataframe of batters.

    Returns:
        DataFrame: A dataframe of batters with an additional column Not Out.
    """
    data = data.dropna(inplace=False)
    indexes_to_drop: list = list(
        data.loc[data["Player"].str.contains(r"Did not bat")].index
    )
    indexes_to_drop.extend(
        list(data.loc[data["Player"].str.contains(r"Fall of wickets")].index)
    )
    data = data.drop(index=indexes_to_drop)
    data = data.reset_index(drop=True)
    return data


def clean_sr(data: pd.DataFrame) -> pd.DataFrame:
    """Cleans the SR column of the Dataframe.

    Args:
        data (DataFrame): A dataframe of batters.

    Returns:
        DataFrame: A dataframe of batters with an additional column Not Out.
    """
    data.replace("-", 0, inplace=True)
    return data


def clean_player(data: pd.DataFrame) -> pd.DataFrame:
    """Cleans the Player column of the Dataframe.

    Args:
        data (DataFrame): A dataframe of batters.

    Returns:
        DataFrame: A dataframe of batters with an additional column Not Out.
    """
    data["Player"] = (
        data["Player"].str.replace(r"(†|\(c\))", "", regex=True).str.strip()
    )
    return data


def add_not_out_column(data: pd.DataFrame) -> pd.DataFrame:
    """Adds a new column to the dataframe, true if the Status column is "not out".

    Args:
        data (DataFrame): A dataframe of batters.

    Returns:
        DataFrame: A dataframe of batters with an additional column Not Out.
    """
    data["Not Out"] = np.where(data["Status"] == "not out", True, False)
    return data


def clarify_types(data: pd.DataFrame) -> pd.DataFrame:
    """Specifies column types for the dataframe.

    Args:
        data (DataFrame): A dataframe of batters.

    Returns:
        DataFrame: A dataframe of batters with columns type explicitly defined.
    """
    data = data.astype(
        {
            "Player": "string",
            "Status": "string",
            "R": int,
            "B": int,
            "M": int,
            "4s": int,
            "6s": int,
            "SR": float,
            "Not Out": int,
        }
    )
    return data


def clean(data: pd.DataFrame) -> pd.DataFrame:
    """Cleans a dataframe.

    Args:
        data (DataFrame): A dataframe of batters.

    Returns:
        DataFrame: A clean dataframe of batters.
    """
    data = drop_columns(data)
    data = rename_columns(data)
    data = clean_rows(data)
    data = clean_sr(data)
    data = clean_player(data)
    data = add_not_out_column(data)
    data = clarify_types(data)
    return data


def group(data: pd.DataFrame) -> pd.DataFrame:
    """Groups a dataframe by batter.

    Parameters:
        data (DataFrame): A clean dataframe of batters.

    Returns:
        DataFrame: A dataframe grouped by batter.
    """
    grouped_data: pd.DataFrame = data.groupby("Player")[
        ["R", "B", "4s", "6s", "Not Out"]
    ].sum()
    return grouped_data


def transform(data: pd.DataFrame) -> pd.DataFrame:
    """Cleans and groups a dataframe by batter.

    Args:
        data (DataFrame): A dataframe of batters.

    Returns:
        DataFrame: A cleaned dataframe, grouped by batter.
    """
    cleaned_data: pd.DataFrame = clean(data)
    grouped_data: pd.DataFrame = group(cleaned_data)
    return grouped_data


def obtain_and_transform():
    """Loads unclean batter csv into a dataframe, transforms it and saves it in a new csv.

    Args:
        None.

    Returns:
        None.
    """
    data = pd.read_csv("./data/extracted_data.csv")
    transformed_data = transform(data)
    transformed_data.to_csv("./data/transformed_data.csv")
