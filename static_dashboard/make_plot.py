import os

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.pyplot.switch_backend("Agg")


def run():
    _make_plot_status_count_per_day()


def _make_plot_status_count_per_day():
    status_count_per_day = _compute_status_count_per_day()
    _plot_status_count_per_day(status_count_per_day)


def _plot_status_count_per_day(status_count_per_day):

    plt.figure(figsize=(12, 6))

    for column in status_count_per_day.columns:
        plt.plot(
            status_count_per_day.index.tolist(),
            status_count_per_day[column],
            label=column,
        )

    plt.savefig("static/report.png")


def _compute_status_count_per_day():
    requests_table = _load_rdbms_requests_table()
    last_4_weeks_requests = _select_last_4_weeks_requests(requests_table)
    status_count_per_day = last_4_weeks_requests.groupby(
        ["open_date", "status"], as_index=False
    ).size()
    status_count_per_day = status_count_per_day.pivot(
        index="open_date", columns="status", values="size"
    )
    status_count_per_day = status_count_per_day.fillna(0)
    return status_count_per_day


def _load_rdbms_requests_table():
    module_path = os.path.dirname(__file__)
    filename = os.path.join(module_path, "../operational_rdbms/requests_table.csv")
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File {filename} not found")
    data = pd.read_csv(filename, sep=",")
    return data


def _select_last_4_weeks_requests(requests_table):
    requests_table = requests_table.copy()
    today = requests_table.open_date.tail(1).values[0]
    today = pd.to_datetime(today)
    last_week = today - pd.Timedelta(days=7 * 4)
    last_week = last_week.strftime("%Y-%m-%d")
    last_week_requests = requests_table[requests_table.open_date >= last_week]
    return last_week_requests


if __name__ == "__main__":
    run()
