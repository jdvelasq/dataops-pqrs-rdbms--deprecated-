import os
from datetime import datetime

import pandas as pd

# -----------------------------------------------------------------------------
# parametros de la simulación
assign_rate_per_person = 100
in_progress_rate_per_person = 15
team_size = 8


def process_next_weeks(n_weeks):
    for _ in range(n_weeks):
        process_next_week()


def process_next_week():
    rdbms_requests_table = load_rdbms_requests_table()
    historical_requests_table = load_historial_requests_table()
    last_procesed_date = rdbms_requests_table.open_date.tail(1).values[0]
    batch = historical_requests_table[
        historical_requests_table.open_date > last_procesed_date
    ]
    batch = select_next_week(batch)
    batch = assign_last_modified_field(batch)
    rdbms_requests_table = pd.concat([rdbms_requests_table, batch])
    rdbms_requests_table = process_rdbms_request_table(rdbms_requests_table)
    overwrite_rdbms_requests_table(rdbms_requests_table)
    print(rdbms_requests_table.loc[batch.index, :])


def select_next_week(batch_data):
    batch_data = batch_data.copy()
    current_date = batch_data.open_date.head(1).values[0]
    next_date = pd.to_datetime(current_date) + pd.Timedelta(days=7)
    next_day = next_date.strftime("%A").lower()
    next_date = next_date.strftime("%Y-%m-%d")
    while next_day != "monday":
        next_date = compute_next_day(next_date)
        next_day = pd.to_datetime(next_date).strftime("%A").lower()
    batch_data = batch_data[batch_data.open_date < next_date]
    return batch_data


def restart(restart_date="2017-09-14"):
    historial_requests_table = load_historial_requests_table()
    requests_table = select_initial_request_table(
        historial_requests_table, restart_date
    )
    requests_table = process_rdbms_request_table(requests_table)
    overwrite_rdbms_requests_table(requests_table)
    print(requests_table)


def process_rdbms_request_table(table):
    current_date = get_init_business_date(table)
    last_date = table.open_date.tail(1).values[0]
    daily_assign_capacity, max_in_progress_capacity = compute_team_capacity(table)
    while current_date <= last_date:
        table = process_current_date(
            table, current_date, daily_assign_capacity, max_in_progress_capacity
        )
        current_date = compute_next_day(current_date)
    return table


def compute_team_capacity(table):
    global team_size
    global in_progress_rate_per_person
    global assign_rate_per_person
    assign_team = 1
    # num_open_tasks = len(table[table.status == "open"])
    # assign_team = max(1, int(10 * num_open_tasks / assign_rate_per_person) / 10)
    # assign_team = max(1, assign_team)
    in_progress_team = team_size - assign_team
    daily_assign_capacity = int(assign_rate_per_person * assign_team)
    max_in_progress_capacity = int(in_progress_team * in_progress_rate_per_person)

    return daily_assign_capacity, max_in_progress_capacity


def process_current_date(
    table, current_date, daily_assign_capacity, max_in_progress_capacity
):

    batch_data = table[table.status != "closed"].copy()
    batch_data = batch_data[batch_data.open_date <= current_date]

    # daily_assign_capacity = random.randint(int(80), int(100))
    # max_in_progress_capacity = random.randint(8 * int(80), 8 * int(100))

    #
    # open ---> assigned (current day)
    #
    open_requests = batch_data[batch_data.status == "open"]
    n = min(daily_assign_capacity, len(open_requests))
    indexes = open_requests.index.values[:n]
    batch_data.loc[indexes, "status"] = "assigned"
    batch_data.loc[indexes, "assigned_date"] = current_date

    #
    # in progress ---> closed (current day)
    #
    in_progress_requests = batch_data[batch_data.status == "in progress"]
    batch_data.loc[in_progress_requests.index, "age"] -= 1
    batch_data.loc[batch_data.age == 0, "status"] = "closed"
    batch_data.loc[batch_data.age == 0, "closed_date"] = current_date

    #
    # assigned ---> in progress (current day)
    #
    in_progress_requests = batch_data[batch_data.status == "in progress"]
    n = min(
        max_in_progress_capacity - len(in_progress_requests), len(in_progress_requests)
    )
    assigned_requests = batch_data[batch_data.status == "assigned"]
    n = max(n, len(assigned_requests))
    indexes = assigned_requests.index.values[:n]
    batch_data.loc[indexes, "status"] = "in progress"
    batch_data.loc[indexes, "in_progress_date"] = current_date

    #
    # copy data
    #
    table.loc[batch_data.index, "status"] = batch_data.status.values
    table.loc[batch_data.index, "assigned_date"] = batch_data.assigned_date.values
    table.loc[batch_data.index, "in_progress_date"] = batch_data.in_progress_date.values
    table.loc[batch_data.index, "closed_date"] = batch_data.closed_date.values
    table.loc[batch_data.index, "age"] = batch_data.age.values

    return table


def get_init_business_date(table):
    table = table.copy()
    assigned_date = table.assigned_date.dropna()
    if len(assigned_date) == 0:
        assigned_date = table.open_date.head().values[0]
        assigned_date = repair_business_day(assigned_date)
    else:
        assigned_date = assigned_date.tail(1).values[0]
        assigned_date = compute_next_day(assigned_date)
    return assigned_date


def repair_business_day(date):
    date = pd.to_datetime(date)
    day_name = date.strftime("%A").lower()
    if day_name == "saturday":
        date = date + pd.Timedelta(days=2)
    elif day_name == "sunday":
        date = date + pd.Timedelta(days=1)
    date = date.strftime("%Y-%m-%d")
    return date


def compute_next_day(date):
    date = pd.to_datetime(date) + pd.Timedelta(days=1)
    date = repair_business_day(date)
    return date


def load_rdbms_requests_table():
    module_path = os.path.dirname(__file__)
    filename = os.path.join(module_path, "../operational_rdbms/requests_table.csv")
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File {filename} not found")
    data = pd.read_csv(filename, sep=",")
    return data


def overwrite_rdbms_requests_table(data):
    module_path = os.path.dirname(__file__)
    folder_path = os.path.join(module_path, "../operational_rdbms")
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    filename = os.path.join(folder_path, "requests_table.csv")
    data.to_csv(filename, sep=",", index=False)


def select_initial_request_table(historical_request_table, restart_date):
    historical_request_table = historical_request_table.copy()
    rdbms_request_table = historical_request_table[
        historical_request_table.open_date <= restart_date
    ]
    rdbms_request_table = assign_last_modified_field(rdbms_request_table)
    return rdbms_request_table


def assign_last_modified_field(table):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    table = table.assign(last_modified=now)
    return table


def load_historial_requests_table():
    module_path = os.path.dirname(__file__)
    filename = os.path.join(module_path, "historical_requests_table.csv")
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File {filename} not found")
    data = pd.read_csv(filename, sep=",")
    return data
