# packages
import os
import json
import requests
import tempfile
import numpy as np
import pandas as pd

# from repo
import library.python.morta.api as ma


# convert morta rows to dataframe:
def morta_rows_to_dataframe(
    input_morta_rows: list, with_row_id: bool = False
) -> pd.DataFrame:
    if with_row_id is False:
        input_rows = list(row["rowData"] for row in input_morta_rows)
    else:
        input_rows = []
        for row in input_morta_rows:
            current_row = row["rowData"]
            current_row["rowId"] = row["publicId"]
            input_rows.append(current_row)
    df = pd.DataFrame(input_rows)
    return df


# convert dataframe to morta rows:
# takes in a dataframe consisting of columns and rows
# outputs a list of morta rowData format (can be used for insert, update rows, upsert)
# example output:
# [
#     {
#     "rowData":
#         {
#             "First column": "Red",
#             "Scond column": "Green"
#         }
#     }
# ]
def dataframe_to_morta_rows(input_df: pd.DataFrame, index_column: str = "") -> list:
    # this is done to remove all empty strings and replace by Nan to insert None into Morta cells
    input_df = input_df.replace("", np.NaN)
    # this is done to make sure that the dataframe is flattened (no grouped hierarchical index)
    input_df = input_df.reset_index(drop=True)
    input_json = input_df.to_json(orient="records", index=True, default_handler=str)
    input_json_list = json.loads(input_json)
    rows = list({"rowData": row} for row in input_json_list)
    return rows


def dataframe_to_morta_update_cells(
    input_df: pd.DataFrame, row_id_column_name: str, update_columns: list
) -> list:
    update_cells: list = []

    if input_df.empty:
        return update_cells

    for update_column in update_columns:
        new_df = input_df[[row_id_column_name, update_column]]
        new_df.set_index(row_id_column_name)
        new_df = new_df.rename(
            columns={row_id_column_name: "rowId", update_column: "value"}
        )
        new_df["columnName"] = update_column
        current_json = new_df.to_json(orient="records", index=True)
        json_list = json.loads(current_json)
        update_cells = update_cells + json_list
    return update_cells


def convert_csv_to_pandas(path: str) -> pd.DataFrame:
    dirname = os.path.dirname(os.path.realpath(__file__))
    file_name = os.path.join(dirname, path)
    data = pd.read_csv(file_name, dtype=str)
    return data


def convert_excel_to_pandas(path: str) -> pd.DataFrame:
    dirname = os.path.dirname(os.path.realpath(__file__))
    file_name = os.path.join(dirname, path)
    data = pd.read_excel(file_name, dtype=str)
    return data


def save_csv_from_df(data: pd.DataFrame, path: str, index: int):
    dirname = os.path.dirname(os.path.realpath(__file__))
    file_name = os.path.join(dirname, path)
    data.to_csv(file_name, index=index)


def save_temp_excel_from_df(
    data: pd.DataFrame, path: str, sheet_name: str = "Sheet1"
) -> str:
    f, file_name = tempfile.mkstemp(suffix=".xls", prefix=path)
    data.to_excel(file_name, sheet_name, engine="xlsxwriter")
    return file_name


def save_temp_xlsx_from_df(
    data: pd.DataFrame, path: str, sheet_name: str = "Sheet1"
) -> str:
    f, file_name = tempfile.mkstemp(suffix=".xlsx", prefix=path)
    data.to_excel(file_name, sheet_name, engine="xlsxwriter")
    return file_name


def dataframe_to_list(input_df: pd.DataFrame) -> list:
    input_df = input_df.reset_index(drop=True)
    input_json = input_df.to_json(orient="records", index=True)
    result = json.loads(input_json)
    return result


def get_columns_not_in_df(df: pd.DataFrame, columns: list) -> list:
    """
    Purpose
    ----------
    Gets a list of columns not in the dataframe.

    Can be used to check the output of functions to see if all the needed columns in the output are there

    Parameters
    ----------
    - df: a pandas dataframe
    - columns: a list of string column names

    Output
    ----------
    - list of columns which are not in the dataframe
    """
    # get the columns of the df in a list
    existing_columns = df.columns.values.tolist()

    # get the difference between the two lists
    return list(set(columns).difference(existing_columns))


def get_workbook_from_json(file_json: dict) -> pd.ExcelFile:
    tokenized_url = ma.get_file(file_json["url"])["url"]
    binary_file = requests.get(tokenized_url).content
    if file_json["extension"] == "xls":
        return pd.ExcelFile(binary_file, engine="xlrd")
    elif file_json["extension"] == "xlsx":
        return pd.ExcelFile(binary_file, engine="openpyxl")
    else:
        raise Exception(
            f"Expected xslx or xls extension file. Received: {file_json['extension']}"
        )


def get_dataframe_from_csv(file_json: dict) -> pd.DataFrame:
    tokenized_url = ma.get_file(file_json["url"])["url"]
    if file_json["extension"] == "csv":
        try:
            return pd.read_csv(filepath_or_buffer=tokenized_url)
        except Exception:
            return pd.read_csv(filepath_or_buffer=tokenized_url, encoding="latin1")
    else:
        raise Exception(
            f"Expected csv extension in file. Received: {file_json['extension']}"
        )
