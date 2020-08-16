import pandas as pd
import numpy as np
import os
import re


path_files = r"C:\Users\NQ491DY\Desktop\Data Scientist Bootcamp CESA 2019\Release\03 Introduction to SQL\Hackathon\Challenge 2 - Data Transformation for Process Mining\data"
os.chdir(path_files)

file_source = "caseandevent.csv"
dtypes = {
    "CASE_ID": "str",
    "TIMESTAMP": "str",
    "USNAM": "str",
    "ACTIVITY": "str",
}


df = pd.read_csv(file_source, sep=";", dtype=dtypes)
df["TIMESTAMP"] = df["TIMESTAMP"].astype("datetime64[ns]")


df.set_index(["CASE_ID", "TIMESTAMP"], inplace=True)
df.sort_index(inplace=True)

df_start = df.copy()
df_start = df_start.groupby("CASE_ID", level=0).head(1)
df_start.reset_index(inplace=True)
df_start.loc[:, "START"] = "START"


df_end = df.copy()
df_end = df_end.groupby("CASE_ID", level=0).tail(1)
df_end.reset_index(inplace=True)
df_end.loc[:, "END"] = "END2"

df1 = df.merge(
    df_start[["CASE_ID", "TIMESTAMP", "START"]],
    left_on=["CASE_ID", "TIMESTAMP"],
    right_on=["CASE_ID", "TIMESTAMP"],
    how="left",
)

df1 = df1.merge(
    df_end[["CASE_ID", "TIMESTAMP", "END"]],
    left_on=["CASE_ID", "TIMESTAMP"],
    right_on=["CASE_ID", "TIMESTAMP"],
    how="left",
)

# df1["SOURCE"] = np.nan
# df1["TARGET"] = np.nan
# df1["TIMESTAMP1"] = np.nan
# df1["TIMESTAMP2"] = np.nan
# df1["USER1"] = np.nan
# df1["USER2"] = np.nan

columns_to_use = df1.columns.values.tolist()
df2 = pd.DataFrame(columns=columns_to_use)

# df_gb = df1.groupby("CASE_ID", level=0)
# groups = dict(list(df_gb))

# 2) jednotlive groupnute dfs a pro ne shift


for case, new_df in df1.groupby(["CASE_ID"]):
    print(case)
    new_df.loc[:, "SOURCE"] = new_df["ACTIVITY"]
    new_df.loc[:, "TARGET"] = new_df["ACTIVITY"].shift(periods=-1)
    new_df.loc[:, "TIMESTAMP1"] = new_df["TIMESTAMP"]
    new_df.loc[:, "TIMESTAMP2"] = new_df["TIMESTAMP"].shift(periods=-1)
    new_df.loc[:, "USER1"] = new_df["USNAM"]
    new_df.loc[:, "USER2"] = new_df["USNAM"].shift(periods=-1)
    new_df.dropna(subset=["USER2"], inplace=True)
    new_df.loc[:, "Duration in seconds"] = new_df["TIMESTAMP2"] - new_df["TIMESTAMP1"]
    new_df.loc[:, "Duration in seconds"] = new_df[
        "Duration in seconds"
    ].dt.total_seconds()
    new_df.loc[:, "Duration in minutes"] = new_df["Duration in seconds"] / 60
    new_df.loc[:, "Duration in hours"] = new_df["Duration in minutes"] / 60
    new_df.loc[:, "Duration in days"] = new_df["Duration in hours"] / 24
    new_df.tail(1)["END"] = "END"

    df2 = df2.append(new_df, ignore_index=True, sort=False)


clean_output = [
    "CASE_ID",
    "SOURCE",
    "TIMESTAMP1",
    "USER1",
    "TARGET",
    "TIMESTAMP2",
    "USER2",
    "Duration in days",
    "Duration in hours",
    "Duration in minutes",
    "Duration in seconds",
    "START",
    "END",
]
df2 = df2[clean_output]
df2.to_csv("df1.csv", index=False)

