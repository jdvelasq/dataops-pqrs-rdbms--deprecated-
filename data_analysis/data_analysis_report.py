import os
from datetime import datetime

import pandas as pd

days_of_maximum_term = 10


def main():
    request_table = load_rdbms_requests_table()
    request_table = request_table[request_table.status != "closed"]
    make_status_frequency_report(request_table)
    make_days_elapsed_report(request_table)


def make_days_elapsed_report(request_table):
    days_elapsed_report = compute_days_elapsed_report(request_table)
    write_days_elapsed_report(days_elapsed_report)


def write_days_elapsed_report(days_elapsed_report):
    module_path = os.path.dirname(__file__)
    folder_path = os.path.join(module_path, "../reports")
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    now = datetime.now().strftime("%Y-%m-%d_%H:%M:%S.%f")
    filename = os.path.join(folder_path, f"days_elapsed_{now}.csv")
    days_elapsed_report.to_csv(filename, sep=",", index=False)


def compute_days_elapsed_report(request_table):
    request_table = request_table.copy()
    today = request_table.open_date.tail(1).values[0]
    today = pd.to_datetime(today)
    request_table = request_table.assign(
        days_elapsed=today - pd.to_datetime(request_table.open_date)
    )
    request_table = request_table.assign(
        days_elapsed=request_table.days_elapsed.dt.days
    )
    days_elapsed_report = request_table.days_elapsed.value_counts()
    days_elapsed_report = days_elapsed_report.sort_index(ascending=False)
    days_elapsed_report = days_elapsed_report.to_frame()
    days_elapsed_report = days_elapsed_report.reset_index()
    return days_elapsed_report


def make_status_frequency_report(request_table):
    status_frequency = compute_status_frequency(request_table)
    write_status_frequency_report(status_frequency)


def write_status_frequency_report(status_frequency):
    module_path = os.path.dirname(__file__)
    folder_path = os.path.join(module_path, "../reports")
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    now = datetime.now().strftime("%Y-%m-%d_%H:%M:%S.%f")
    filename = os.path.join(folder_path, f"status_frequency_{now}.csv")
    status_frequency.to_csv(filename, sep=",", index=False)


def compute_status_frequency(request_table):
    request_table = request_table.copy()
    status_frequency = request_table.status.value_counts()
    status_frequency = status_frequency.to_frame()
    status_frequency = status_frequency.reset_index()
    return status_frequency


def load_rdbms_requests_table():
    module_path = os.path.dirname(__file__)
    filename = os.path.join(module_path, "../operational_rdbms/requests_table.csv")
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File {filename} not found")
    data = pd.read_csv(filename, sep=",")
    return data


if __name__ == "__main__":
    main()
