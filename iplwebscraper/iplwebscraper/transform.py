import pandas as pd
import numpy as np
from extract import obtain_batting_results


def clean(data):
    cleaned_data = data.drop(columns=["Unnamed: 8","Unnamed: 9"])
    cleaned_data = cleaned_data.rename(columns={"BATTING":"Player","Unnamed: 1":"Status"})
    cleaned_data = cleaned_data.dropna(inplace=False)
    indexes_to_drop = list(cleaned_data.loc[cleaned_data["Player"].str.contains(r"Did not bat")].index)
    indexes_to_drop.extend(list(cleaned_data.loc[cleaned_data["Player"].str.contains(r"Fall of wickets")].index))
    cleaned_data = cleaned_data.drop(index=indexes_to_drop)
    cleaned_data.replace("-",0, inplace = True)
    cleaned_data["Not Out"] = np.where(cleaned_data["Status"]=="not out",True,False)
    cleaned_data = cleaned_data.astype({"Player":"string", "Status":"string", "R":int, "B":int, "M":int, "4s":int, "6s":int, "SR":float, "Not Out":int})
    return cleaned_data


def group(data):
    grouped_data = data.groupby("Player")[["R","B","4s","6s","Not Out"]].sum()
    return grouped_data


def transform(data):
    cleaned_data = clean(data)
    grouped_data = group(cleaned_data)
    return grouped_data 


if __name__ == "__main__":
    data  = obtain_batting_results()
    transformed_data = transform(data)
    print(transformed_data)