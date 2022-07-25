"""Takes the scraped data, cleans it and
transforms it into the sums of the columns by batter."""
import numpy as np
import pandas as pd


def clean_columns(data):
    """Drops the two empty columns and renames the BATTING and unnamed."""
    cleaned_data = data.drop(columns=["Unnamed: 8", "Unnamed: 9"])
    cleaned_data = cleaned_data.rename(
        columns={"BATTING": "Player", "Unnamed: 1": "Status"}
    )
    return cleaned_data


def clean_rows(cleaned_data):
    """Drops records which are all nan or not batter information.
    Replaces any - fields with 0 so not to distrupt later sum function.
    Then resets the index from 0, drop = True makes sure the old index is not saved
    as a column."""
    cleaned_data = cleaned_data.dropna(inplace=False)
    indexes_to_drop = list(
        cleaned_data.loc[cleaned_data["Player"].str.contains(r"Did not bat")].index
    )
    indexes_to_drop.extend(
        list(
            cleaned_data.loc[
                cleaned_data["Player"].str.contains(r"Fall of wickets")
            ].index
        )
    )
    cleaned_data = cleaned_data.drop(index=indexes_to_drop)
    cleaned_data.replace("-", 0, inplace=True)
    cleaned_data = cleaned_data.reset_index(drop=True)
    return cleaned_data


def add_not_out_column(cleaned_data):
    """Adds a new Not Out column, based on the Status column."""
    cleaned_data["Not Out"] = np.where(cleaned_data["Status"] == "not out", True, False)
    return cleaned_data


def clarify_types(cleaned_data):
    """Specifies the column types."""
    cleaned_data = cleaned_data.astype(
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
    return cleaned_data


def clean(data):
    """Calls the other cleaning functions.
    Returns the newly cleaned data."""
    cleaned_data = clean_columns(data)
    cleaned_data = clean_rows(cleaned_data)
    cleaned_data = add_not_out_column(cleaned_data)
    cleaned_data = clarify_types(cleaned_data)
    return cleaned_data


def group(data):
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
