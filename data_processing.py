# Import Libraries

import sqlite3
import pandas as pd
import numpy as np
import json
from datetime import datetime

# Read data
def read_sqlite_data():
    """
    Read the data using sqlite3 library in python
    """
    sqlite_data = sqlite3.connect("travel.sqlite")
    return sqlite_data

def get_table_list(sqlite_data):
    """
    Queries the names of all the tables in the sql database

    Returns:
        list: list of all the tables names
    """
    sql_query = "SELECT name FROM sqlite_master WHERE type='table';"
    cur = sqlite_data.cursor()
    cur.execute(sql_query)
    table_list = cur.fetchall()
    cleaned_table_list = [table_list[i][0] for i in range(len(table_list))]
    return cleaned_table_list

def create_dataframes_dict(sqlite_data, cleaned_table_list):
    """
    transforms all the tables of the database into pandas dataframes

    Args:
        sqlite_data (_type_): _description_
        cleaned_table_list (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Transforms the result of the sql query into a data frame
    dfs_dict = {cleaned_table_list[i]: pd.read_sql_query("SELECT * FROM "+cleaned_table_list[i], 
                                    sqlite_data) for i in range(len(cleaned_table_list))}
    return dfs_dict

def merge_dfs(dfs_dict):
    """
    returns a complete dataframe merging all the dataframes/tables based on their primary 
    and secondary keys

    Args:
        dfs_dict (dict): dictionary containing all the dataframes

    Returns:
        pandas df: df containing the merged data frame
    """
    all_tickets_data = dfs_dict['ticket_flights'].merge(dfs_dict['flights'], on = "flight_id").merge(
        dfs_dict["aircrafts_data"], on = "aircraft_code").merge(
        dfs_dict["airports_data"], left_on = "departure_airport", right_on = "airport_code", 
        suffixes=("", "_departure")).merge(dfs_dict["airports_data"], left_on = "arrival_airport", right_on = "airport_code", 
        suffixes=("", "_arrival")).merge(dfs_dict["tickets"], on = "ticket_no").merge(
        dfs_dict["bookings"], on= "book_ref"
        )
    return all_tickets_data

def remove_keys(all_tickets_data):
    cleaned_tickets_data = all_tickets_data.drop(
        ["flight_id", "aircraft_code", "airport_code", "airport_code_arrival",
        "ticket_no", "book_ref", "flight_no", "passenger_id"], axis = 1)
    return cleaned_tickets_data

def remove_irrelevent_columns(cleaned_tickets_data):
    relevent_tickets_data = cleaned_tickets_data.drop(["status", "timezone", "coordinates",
                                                        "coordinates_arrival", "timezone_arrival",
                                                        "actual_arrival", "actual_departure"], axis = 1)
    return relevent_tickets_data

def rename_columns(relevent_tickets_data):
    relevent_tickets_data = relevent_tickets_data.rename({"airport_name": "airport_name_departure", 
                                                            "city": "city_departure"})
    return relevent_tickets_data

def convert_column_to_dict(data, column_names):
    """
    Convert column from str to dict
    """
    for column_name in column_names:
        data[column_name] = data[column_name].map(lambda x: json.loads(x))
    return data

def english_formatting(data, column_names):
    """
    pick only the english names
    """
    for column_name in column_names:
        data[column_name] = data[column_name].apply(lambda x: x['en'])
    return data

# Main
if __name__ == "__main__":
    
    sqlite_data = read_sqlite_data()
    
    cleaned_table_list = get_table_list(sqlite_data)
    
    dfs_dict = create_dataframes_dict(sqlite_data, cleaned_table_list)
    
    all_tickets_data = merge_dfs(dfs_dict)
    
    cleaned_tickets_data = remove_keys(all_tickets_data)
    
    relevent_tickets_data = remove_irrelevent_columns(cleaned_tickets_data)
    
    relevent_tickets_data = rename_columns(relevent_tickets_data)
    
    formatted_tickets_data = convert_column_to_dict(relevent_tickets_data, ["model", 
                        "airport_name_departure", "city_departure", "airport_name_arrival",
                        "city_arrival"])
    
    en_formatted_tickets_data = english_formatting(formatted_tickets_data, ["model", 
                        "airport_name_departure", "city_departure", "airport_name_arrival",
                        "city_arrival"])