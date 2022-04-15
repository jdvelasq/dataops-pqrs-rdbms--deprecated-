import os
from datetime import datetime

import pandas as pd

days_of_maximum_term = 10


def main():
    request_table = load_rdbms_requests_table()
    report_table = compute_report_table(request_table)
    save_report_table(report_table)


def save_report_table(report_table):
    module_path = os.path.dirname(__file__)
    folder_path = os.path.join(module_path, "../reports")
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    now = datetime.now().strftime("%Y-%m-%d_%H:%M:%S.%f")
    filename = os.path.join(folder_path, f"internal_control_{now}.csv")
    report_table.to_csv(filename, sep=",", index=False)


def compute_report_table(request_table):

    request_table = request_table[request_table.status == "closed"]
    request_table = request_table.assign(
        n_days=pd.to_datetime(request_table.closed_date)
        - pd.to_datetime(request_table.open_date)
    )
    request_table = request_table.assign(n_days=request_table.n_days.dt.days)
    request_table = request_table.sort_values(by="n_days", ascending=False)
    request_table = request_table.assign(
        delay=request_table.n_days - days_of_maximum_term
    )
    request_table = request_table.assign(
        delay=request_table.delay.apply(lambda x: x if x > 0 else 0)
    )
    request_table = request_table[request_table.delay > 0]
    request_table = request_table[["record_id", "open_date", "closed_date", "delay"]]

    return request_table


def load_rdbms_requests_table():
    module_path = os.path.dirname(__file__)
    filename = os.path.join(module_path, "../operational_rdbms/requests_table.csv")
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File {filename} not found")
    data = pd.read_csv(filename, sep=",")
    return data


if __name__ == "__main__":
    main()
