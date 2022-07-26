"""Takes the scraped data, cleans it and
transforms it into the sums of the columns by batter."""
import numpy as np
import pandas as pd


def clean_columns(data: pd.DataFrame) -> pd.DataFrame:
    """Drops the two empty columns and renames the BATTING and unnamed."""
    cleaned_data = data.drop(columns=["Unnamed: 8", "Unnamed: 9"])
    cleaned_data = cleaned_data.rename(
        columns={"BATTING": "Player", "Unnamed: 1": "Status"}
    )
    return cleaned_data


def clean_rows(data: pd.DataFrame) -> pd.DataFrame:
    """Drops records which are all nan or not batter information.
    Replaces any - fields with 0 so not to distrupt later sum function.
    Then resets the index from 0, drop = True makes sure the old index is not saved
    as a column."""
    data = data.dropna(inplace=False)
    indexes_to_drop = list(
        data.loc[data["Player"].str.contains(r"Did not bat")].index
    )
    indexes_to_drop.extend(
        list(
            data.loc[
                data["Player"].str.contains(r"Fall of wickets")
            ].index
        )
    )
    data = data.drop(index=indexes_to_drop)
    data.replace("-", 0, inplace=True)
    data = data.reset_index(drop=True)
    return data


def add_not_out_column(data: pd.DataFrame) -> pd.DataFrame:
    """Adds a new Not Out column, based on the Status column."""
    data["Not Out"] = np.where(data["Status"] == "not out", True, False)
    return data


def clarify_types(data: pd.DataFrame) -> pd.DataFrame:
    """Specifies the column types."""
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
    """Calls the other cleaning functions.
    Returns the newly cleaned data."""
    cleaned_data = clean_columns(data)
    cleaned_data = clean_rows(cleaned_data)
    cleaned_data = add_not_out_column(cleaned_data)
    cleaned_data = clarify_types(cleaned_data)
    return cleaned_data


def group(data: pd.DataFrame) -> pd.DataFrame:
    """Creates the summed fields by player"""
    grouped_data = data.groupby("Player")[["R", "B", "4s", "6s", "Not Out"]].sum()
    return grouped_data


def transform():
    """Loads the cleaned data from the csv,
    which is then grouped and then saved in another csv."""
    data = pd.read_csv("extracted_data.csv")
    cleaned_data = clean(data)
    grouped_data = group(cleaned_data)
    grouped_data.to_csv("transformed_data.csv")


if __name__ == "__main__":
    transform()
