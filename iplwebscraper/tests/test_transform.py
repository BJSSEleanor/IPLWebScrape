"""Tests for the transform function."""
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long
import pytest
import pandas as pd
import numpy as np
from iplwebscraper.transform import (
    clean_columns,
    clean_rows,
    add_not_out_column,
    clean,
    group
)


@pytest.fixture
def dataframe() -> pd.DataFrame:
    """Defines a base example dataframe for the tests."""
    example = {
        "BATTING": [
            "Yashasvi Jaiswal",
            np.nan,
            "Jos Buttler",
            np.nan,
            "Sanju Samson (c)†",
            np.nan,
            "Devdutt Padikkal",
            np.nan,
            "Shimron Hetmyer",
            np.nan,
            "Ravichandran Ashwin",
            np.nan,
            "Obed McCoy",
            np.nan,
            "Prasidh Krishna",
            "Extras",
            "TOTAL",
            "Did not bat: Yuzvendra Chahal",
            "Fall of wickets: 1-31 (Yashasvi Jaiswal, 3.6 ov), 2-60 (Sanju Samson, 8.2 ov), 3-79 (Devdutt Padikkal, 11.5 ov), 4-79 (Jos Buttler, 12.1 ov), 5-94 (Shimron Hetmyer, 14.6 ov), 6-98 (Ravichandran Ashwin, 15.5 ov), 7-112 (Trent Boult, 17.3 ov), 8-130 (Obed McCoy, 19.4 ov), 9-130 (Riyan Parag, 19.6 ov)",
        ],
        "Unnamed: 1": [
            "c Sai Kishore b Yash Dayal",
            np.nan,
            "c †Saha b Pandya",
            np.nan,
            "c Sai Kishore b Pandya",
            np.nan,
            "c Mohammed Shami b Rashid Khan",
            np.nan,
            "c & b Pandya",
            np.nan,
            "c Miller b Sai Kishore",
            np.nan,
            "run out (Tewatia/Mohammed Shami)",
            np.nan,
            "not out",
            "(lb 2)",
            "20 Ov (RR: 6.50)",
            "Did not bat: Yuzvendra Chahal",
            "Fall of wickets: 1-31 (Yashasvi Jaiswal, 3.6 ov), 2-60 (Sanju Samson, 8.2 ov), 3-79 (Devdutt Padikkal, 11.5 ov), 4-79 (Jos Buttler, 12.1 ov), 5-94 (Shimron Hetmyer, 14.6 ov), 6-98 (Ravichandran Ashwin, 15.5 ov), 7-112 (Trent Boult, 17.3 ov), 8-130 (Obed McCoy, 19.4 ov), 9-130 (Riyan Parag, 19.6 ov)",
        ],
        "R": [
            22,
            np.nan,
            39,
            np.nan,
            14,
            np.nan,
            2,
            np.nan,
            11,
            np.nan,
            6,
            np.nan,
            8,
            np.nan,
            0,
            2,
            130 / 9,
            "Did not bat: Yuzvendra Chahal",
            "Fall of wickets: 1-31 (Yashasvi Jaiswal, 3.6 ov), 2-60 (Sanju Samson, 8.2 ov), 3-79 (Devdutt Padikkal, 11.5 ov), 4-79 (Jos Buttler, 12.1 ov), 5-94 (Shimron Hetmyer, 14.6 ov), 6-98 (Ravichandran Ashwin, 15.5 ov), 7-112 (Trent Boult, 17.3 ov), 8-130 (Obed McCoy, 19.4 ov), 9-130 (Riyan Parag, 19.6 ov)",
        ],
        "B": [
            16,
            np.nan,
            35,
            np.nan,
            11,
            np.nan,
            10,
            np.nan,
            12,
            np.nan,
            9,
            np.nan,
            5,
            np.nan,
            0,
            np.nan,
            np.nan,
            "Did not bat: Yuzvendra Chahal",
            "Fall of wickets: 1-31 (Yashasvi Jaiswal, 3.6 ov), 2-60 (Sanju Samson, 8.2 ov), 3-79 (Devdutt Padikkal, 11.5 ov), 4-79 (Jos Buttler, 12.1 ov), 5-94 (Shimron Hetmyer, 14.6 ov), 6-98 (Ravichandran Ashwin, 15.5 ov), 7-112 (Trent Boult, 17.3 ov), 8-130 (Obed McCoy, 19.4 ov), 9-130 (Riyan Parag, 19.6 ov)",
        ],
        "M": [
            22,
            np.nan,
            64,
            np.nan,
            21,
            np.nan,
            20,
            np.nan,
            20,
            np.nan,
            22,
            np.nan,
            12,
            np.nan,
            2,
            np.nan,
            np.nan,
            "Did not bat: Yuzvendra Chahal",
            "Fall of wickets: 1-31 (Yashasvi Jaiswal, 3.6 ov), 2-60 (Sanju Samson, 8.2 ov), 3-79 (Devdutt Padikkal, 11.5 ov), 4-79 (Jos Buttler, 12.1 ov), 5-94 (Shimron Hetmyer, 14.6 ov), 6-98 (Ravichandran Ashwin, 15.5 ov), 7-112 (Trent Boult, 17.3 ov), 8-130 (Obed McCoy, 19.4 ov), 9-130 (Riyan Parag, 19.6 ov)",
        ],
        "4s": [
            1,
            np.nan,
            5,
            np.nan,
            2,
            np.nan,
            0,
            np.nan,
            2,
            np.nan,
            0,
            np.nan,
            0,
            np.nan,
            0,
            np.nan,
            np.nan,
            "Did not bat: Yuzvendra Chahal",
            "Fall of wickets: 1-31 (Yashasvi Jaiswal, 3.6 ov), 2-60 (Sanju Samson, 8.2 ov), 3-79 (Devdutt Padikkal, 11.5 ov), 4-79 (Jos Buttler, 12.1 ov), 5-94 (Shimron Hetmyer, 14.6 ov), 6-98 (Ravichandran Ashwin, 15.5 ov), 7-112 (Trent Boult, 17.3 ov), 8-130 (Obed McCoy, 19.4 ov), 9-130 (Riyan Parag, 19.6 ov)",
        ],
        "6s": [
            2,
            np.nan,
            0,
            np.nan,
            0,
            np.nan,
            0,
            np.nan,
            0,
            np.nan,
            0,
            np.nan,
            1,
            np.nan,
            0,
            np.nan,
            np.nan,
            "Did not bat: Yuzvendra Chahal",
            "Fall of wickets: 1-31 (Yashasvi Jaiswal, 3.6 ov), 2-60 (Sanju Samson, 8.2 ov), 3-79 (Devdutt Padikkal, 11.5 ov), 4-79 (Jos Buttler, 12.1 ov), 5-94 (Shimron Hetmyer, 14.6 ov), 6-98 (Ravichandran Ashwin, 15.5 ov), 7-112 (Trent Boult, 17.3 ov), 8-130 (Obed McCoy, 19.4 ov), 9-130 (Riyan Parag, 19.6 ov)",
        ],
        "SR": [
            137.50,
            np.nan,
            111.42,
            np.nan,
            127.27,
            np.nan,
            20.00,
            np.nan,
            91.66,
            np.nan,
            66.66,
            np.nan,
            160.00,
            np.nan,
            "-",
            np.nan,
            np.nan,
            "Did not bat: Yuzvendra Chahal",
            "Fall of wickets: 1-31 (Yashasvi Jaiswal, 3.6 ov), 2-60 (Sanju Samson, 8.2 ov), 3-79 (Devdutt Padikkal, 11.5 ov), 4-79 (Jos Buttler, 12.1 ov), 5-94 (Shimron Hetmyer, 14.6 ov), 6-98 (Ravichandran Ashwin, 15.5 ov), 7-112 (Trent Boult, 17.3 ov), 8-130 (Obed McCoy, 19.4 ov), 9-130 (Riyan Parag, 19.6 ov)",
        ],
        "Unnamed: 8": [
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "Did not bat: Yuzvendra Chahal",
            "Fall of wickets: 1-31 (Yashasvi Jaiswal, 3.6 ov), 2-60 (Sanju Samson, 8.2 ov), 3-79 (Devdutt Padikkal, 11.5 ov), 4-79 (Jos Buttler, 12.1 ov), 5-94 (Shimron Hetmyer, 14.6 ov), 6-98 (Ravichandran Ashwin, 15.5 ov), 7-112 (Trent Boult, 17.3 ov), 8-130 (Obed McCoy, 19.4 ov), 9-130 (Riyan Parag, 19.6 ov)",
        ],
        "Unnamed: 9": [
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "Did not bat: Yuzvendra Chahal",
            "Fall of wickets: 1-31 (Yashasvi Jaiswal, 3.6 ov), 2-60 (Sanju Samson, 8.2 ov), 3-79 (Devdutt Padikkal, 11.5 ov), 4-79 (Jos Buttler, 12.1 ov), 5-94 (Shimron Hetmyer, 14.6 ov), 6-98 (Ravichandran Ashwin, 15.5 ov), 7-112 (Trent Boult, 17.3 ov), 8-130 (Obed McCoy, 19.4 ov), 9-130 (Riyan Parag, 19.6 ov)",
        ],
    }
    dataframe = pd.DataFrame(example)
    return dataframe


def test_clean_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Tests the clean_columns function to see if the correct columns are dropped/renamed."""
    cleaned_columns = clean_columns(dataframe)
    actual_columns = list(cleaned_columns.columns)
    expected_columns = ["Player", "Status", "R", "B", "M", "4s", "6s", "SR"]
    assert actual_columns == expected_columns


def test_clean_rows(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Tests the clean_rows function to see if the correct number of rows are dropped."""
    cleaned_columns = clean_columns(dataframe)
    cleaned_rows = clean_rows(cleaned_columns)
    actual_rows = len(cleaned_rows.index)
    expected_rows = 8
    assert actual_rows == expected_rows


def test_add_new_column(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Tests the add_not_out function to see if adds a new column."""
    cleaned_columns = clean_columns(dataframe)
    cleaned_rows = clean_rows(cleaned_columns)
    new_column_added = add_not_out_column(cleaned_rows)
    actual_columns = list(new_column_added.columns)
    expected_columns = ["Player", "Status", "R", "B", "M", "4s", "6s", "SR", "Not Out"]
    assert actual_columns == expected_columns


def test_add_new_column_values_correct(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Tests the add_not_out function to see if the values in the new column have been calculated correctly."""
    cleaned_columns = clean_columns(dataframe)
    cleaned_rows = clean_rows(cleaned_columns)
    new_column_added = add_not_out_column(cleaned_rows)
    actual_values = list(new_column_added["Not Out"])
    expected_values = [False, False, False, False, False, False, False, True]
    assert actual_values == expected_values


@pytest.fixture
def cleaned_dataframe() -> pd.DataFrame:
    """Defines a base example cleaned dataframe for the tests."""
    example = {
        "Player": [
            "Yashasvi Jaiswal",
            "Jos Buttler",
            "Sanju Samson (c)†",
            "Devdutt Padikkal",
            "Shimron Hetmyer",
            "Ravichandran Ashwin",
            "Obed McCoy",
            "Prasidh Krishna",
        ],
        "Status": [
            "c Sai Kishore b Yash Dayal",
            "c †Saha b Pandya",
            "c Sai Kishore b Pandya",
            "c Mohammed Shami b Rashid Khan",
            "c & b Pandya",
            "c Miller b Sai Kishore",
            "run out (Tewatia/Mohammed Shami)",
            "not out",
        ],
        "R": [22, 39, 14, 2, 11, 6, 8, 0],
        "B": [16, 35, 11, 10, 12, 9, 5, 0],
        "M": [22, 64, 21, 20, 20, 22, 12, 2],
        "4s": [1, 5, 2, 0, 2, 0, 0, 0],
        "6s": [2, 0, 0, 0, 0, 0, 1, 0],
        "SR": [137.50, 111.42, 127.27, 20.00, 91.66, 6.66, 160.00, 0.00],
        "Not Out": [0, 0, 0, 0, 0, 0, 0, 1],
    }
    dataframe = pd.DataFrame(example)
    dataframe = dataframe.astype(
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
    return dataframe


def test_clarify_types(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Tests the clarify_types function to see if the dataframe dtypes are as expected."""
    expected_result = "[string[python], string[python], dtype('int32'), dtype('int32'), dtype('int32'), dtype('int32'), dtype('int32'), dtype('float64'), dtype('int32')]"
    result = clean(dataframe)
    result = result.reset_index(drop=True)
    actual_result = list(result.dtypes)
    assert str(actual_result) == expected_result


def test_clean(dataframe: pd.DataFrame, cleaned_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Tests the clean function to see if the above functions cleans as expected when used altogether."""
    expected_result = cleaned_dataframe
    actual_result = clean(dataframe)
    actual_result = actual_result.reset_index(drop=True)
    assert actual_result.equals(expected_result) is True


def test_group(dataframe: pd.DataFrame, cleaned_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Tests the group function to see if the dataframe is grouped and the sums calculated correctly."""
    expected_result = cleaned_dataframe
    expected_result = expected_result.groupby("Player")[
        ["R", "B", "4s", "6s", "Not Out"]
    ].sum()
    actual_result = group(clean(dataframe))
    assert actual_result.equals(expected_result) is True
