"""Tests for the transform function.

Fixtures:
    base_dataframe(None) -> pd.DataFrame
    cleaned_columns_dataframe(None) -> pd.DataFrame
    clean_rows_dataframe(None) -> pd.DataFrame
    sr_player_cleaned_dataframe(None) -> pd.DataFrame
    new_column_dataframe(None) -> pd.DataFrame
    cleaned_dataframe(None) -> pd.DataFrame

Functions:
    test_drop_columns(Dataframe) -> None
    test_rename_columns(Dataframe) -> None
    test_clean_rows(Dataframe) -> None
    test_clean_sr(Dataframe) -> None
    test_clean_player(Dataframe) -> None
    test_add_not_out_column(Dataframe) -> None
    test_clarify_types(Dataframe) -> None
    test_clean(Dataframe) -> None
    test_group(Dataframe) -> None

"""
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
from iplwebscraper.transform import (
    drop_columns,
    rename_columns,
    clean_rows,
    clean_sr,
    clean_player,
    add_not_out_column,
    clarify_types,
    clean,
    group,
)


@pytest.fixture
def base_dataframe() -> pd.DataFrame:
    """
    Defines an unclean, example dataframe for the tests.
        Parameters:
                None.
        Returns:
                dataframe (Dataframe): A clean examplar dataframe.
    """
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
    dataframe = pd.DataFrame(example).astype(object)
    return dataframe


@pytest.fixture
def cleaned_column_dataframe() -> pd.DataFrame:
    """
    Defines an example dataframe with dropped and renamed columns for the tests.
        Parameters:
                None.
        Returns:
                dataframe (Dataframe): A clean examplar dataframe.
    """
    example = {
        "Player": [
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
        "Status": [
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
    }
    dataframe = pd.DataFrame(example).astype(object)
    return dataframe


@pytest.fixture
def clean_rows_dataframe() -> pd.DataFrame:
    """
    Defines an example dataframe with cleaned rows for the tests.
        Parameters:
                None.
        Returns:
                dataframe (Dataframe): A clean examplar dataframe.
    """
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
        "SR": [137.50, 111.42, 127.27, 20.00, 91.66, 66.66, 160.00, "-"],
    }
    dataframe = pd.DataFrame(example).astype(object)
    return dataframe


@pytest.fixture
def sr_player_cleaned_dataframe() -> pd.DataFrame:
    """
    Defines an example dataframe with the cleaned rows and sr and player column for the tests.
        Parameters:
                None.
        Returns:
                dataframe (Dataframe): A clean examplar dataframe.
    """
    example = {
        "Player": [
            "Yashasvi Jaiswal",
            "Jos Buttler",
            "Sanju Samson",
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
        "SR": [137.50, 111.42, 127.27, 20.00, 91.66, 66.66, 160.00, 0.00],
    }
    dataframe = pd.DataFrame(example).astype(
        {
            "Player": object,
            "Status": object,
            "R": object,
            "B": object,
            "M": object,
            "4s": object,
            "6s": object,
            "SR": float,
        }
    )
    return dataframe


@pytest.fixture
def new_column_dataframe() -> pd.DataFrame:
    """
    Defines an example dataframe with the not out column for the tests.
        Parameters:
                None.
        Returns:
                dataframe (Dataframe): A clean examplar dataframe.
    """
    example = {
        "Player": [
            "Yashasvi Jaiswal",
            "Jos Buttler",
            "Sanju Samson",
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
        "SR": [137.50, 111.42, 127.27, 20.00, 91.66, 66.66, 160.00, 0.00],
        "Not Out": [False, False, False, False, False, False, False, True],
    }
    dataframe = pd.DataFrame(example).astype(
        {
            "Player": object,
            "Status": object,
            "R": object,
            "B": object,
            "M": object,
            "4s": object,
            "6s": object,
            "SR": float,
            "Not Out": bool,
        }
    )
    return dataframe


@pytest.fixture
def cleaned_dataframe() -> pd.DataFrame:
    """
    Defines an example cleaned dataframe.
        Parameters:
                None.
        Returns:
                dataframe (Dataframe): A clean examplar dataframe.
    """
    example = {
        "Player": [
            "Yashasvi Jaiswal",
            "Jos Buttler",
            "Sanju Samson",
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
        "SR": [137.50, 111.42, 127.27, 20.00, 91.66, 66.66, 160.00, 0.00],
        "Not Out": [0, 0, 0, 0, 0, 0, 0, 1],
    }
    dataframe = pd.DataFrame(example).astype(
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


def test_drop_columns(base_dataframe: pd.DataFrame):
    """
    Tests drop_columns drops the correct columns.
        Parameters:
                base_dataframe(DataFrame) -> an unclean dataframe
        Returns:
                None.
    """
    cleaned_columns = drop_columns(base_dataframe)
    actual_columns = list(cleaned_columns.columns)
    expected_columns = ["BATTING", "Unnamed: 1", "R", "B", "M", "4s", "6s", "SR"]
    assert actual_columns == expected_columns


def test_rename_columns(base_dataframe: pd.DataFrame):
    """
    Tests rename_columns renames the correct columns.
        Parameters:
                base_dataframe(DataFrame) -> an unclean dataframe
        Returns:
                None.
    """
    cleaned_columns = rename_columns(base_dataframe)
    actual_columns = list(cleaned_columns.columns)
    expected_columns = [
        "Player",
        "Status",
        "R",
        "B",
        "M",
        "4s",
        "6s",
        "SR",
        "Unnamed: 8",
        "Unnamed: 9",
    ]
    assert actual_columns == expected_columns


def test_clean_rows(
    cleaned_column_dataframe: pd.DataFrame, clean_rows_dataframe: pd.DataFrame
):
    """
    Tests clean_rows drops the correct rows.
        Parameters:
                cleaned_column_dataframe(DataFrame) -> the base_dataframe with renamed and the correct no. columns.
                clean_rows_dataframe(DataFrame) -> the cleaned_column_dataframe with the correct rows
        Returns:
                None.
    """
    actual_result = clean_rows(cleaned_column_dataframe)
    expected_result = clean_rows_dataframe
    assert_frame_equal(actual_result, expected_result)


def test_clean_sr(
    clean_rows_dataframe: pd.DataFrame, sr_player_cleaned_dataframe: pd.DataFrame
):
    """
    Tests clean_sr replaces the correct rows in the SR column.
        Parameters:
                clean_rows_dataframe(DataFrame) -> the cleaned_column_dataframe with the correct rows
                sr_player_cleaned_rows_dataframe(DataFrame) -> the clean_rows_dataframe with the correct replacements in SR and Player
        Returns:
                None.
    """
    actual_result = clean_sr(clean_rows_dataframe)
    expected_result = sr_player_cleaned_dataframe
    assert actual_result["SR"].equals(expected_result["SR"]) is True


def test_clean_player(
    clean_rows_dataframe: pd.DataFrame, sr_player_cleaned_dataframe: pd.DataFrame
):
    """
    Tests clean_player replaces the correct rows in the Player column.
        Parameters:
                clean_rows_dataframe(DataFrame) -> the cleaned_column_dataframe with the correct rows
                sr_player_cleaned_rows_dataframe(DataFrame) -> the clean_rows_dataframe with the correct replacements in SR and Player
        Returns:
                None.
    """
    actual_result = clean_player(clean_rows_dataframe)
    expected_result = sr_player_cleaned_dataframe
    assert actual_result["Player"].equals(expected_result["Player"]) is True


def test_clarify_types(new_column_dataframe: pd.DataFrame):
    """
    Tests clarify_types changes the column types correctly.
        Parameters:
                new_column_dataframe(DataFrame) -> the sr_player_cleaned_dataframe with the Not Out column
        Returns:
                None.
    """
    expected = "[string[python], string[python], dtype('int32'), dtype('int32'), dtype('int32'), dtype('int32'), dtype('int32'), dtype('float64'), dtype('int32')]"
    result = clarify_types(new_column_dataframe)
    actual = list(result.dtypes)
    assert str(actual) == expected


def test_add_new_column(
    sr_player_cleaned_dataframe: pd.DataFrame, new_column_dataframe: pd.DataFrame
):
    """
    Tests add_not_out_column adds the new column with the correct values.
        Parameters:
                sr_player_cleaned_rows_dataframe(DataFrame) -> the clean_rows_dataframe with the correct replacements in SR and Player
                new_column_dataframe(DataFrame) -> the sr_player_cleaned_dataframe with the Not Out column
        Returns:
                None.
    """
    actual = add_not_out_column(sr_player_cleaned_dataframe)
    expected = new_column_dataframe
    assert_frame_equal(actual, expected)


def test_clean(base_dataframe: pd.DataFrame, cleaned_dataframe: pd.DataFrame):
    """
    Tests clean function cleans the dataframe correctly.
        Parameters:
                base_dataframe(DataFrame) -> an unclean dataframe
                cleaned_dataframe(DataFrame) -> the new_column_dataframe with the correct types.
        Returns:
                None.
    """
    expected = cleaned_dataframe
    actual = clean(base_dataframe)
    assert_frame_equal(actual, expected)


def test_group(base_dataframe: pd.DataFrame, cleaned_dataframe: pd.DataFrame):
    """
    Tests group function groups the dataframe correctly.
        Parameters:
                base_dataframe(DataFrame) -> an unclean dataframe
                cleaned_dataframe(DataFrame) -> the new_column_dataframe with the correct types.
        Returns:
                None.
    """
    expected = cleaned_dataframe
    expected = expected.groupby("Player")[["R", "B", "4s", "6s", "Not Out"]].sum()
    actual = group(clean(base_dataframe))
    assert_frame_equal(actual, expected)
