#!/usr/bin/python3
import sqlite3
import pandas as pd
from admintools import byte_sizer as human_readable
from matplotlib import pyplot as plt
from datetime import datetime as dt
from typing import Callable
from os.path import islink
from os import listdir

# user defined vars
db_location: str = '/nfs_share/matt_desktop/server_scripts/zm_helper/zm_size.db'
save_fig_location: str = '/nfs_share/matt_desktop/server_scripts/zm_helper/figures/zm_monthly_usage.png'
zm_dir: str = '/var/cache/zoneminder/events'

with sqlite3.connect(db_location) as con:
    df: pd.DataFrame = pd.read_sql('select * from zm_sizes', con)

# only the last 12 months (current month included)
year_months: list[str] = df.date.apply(lambda date_str: date_str[:7]).unique()[-13:]
caches: list[str] = [directory for directory in listdir(zm_dir) if islink(f'{zm_dir}/{directory}')]
camera_data: list[tuple[str, int]] = []

for year_month in year_months:
    size: int = 0  # Disk space size for all cameras (aggregated) for each month (individual) 
    for cache in caches:
        size += df[df.date.str.startswith(year_month)][cache].sum()

    camera_data.append((year_month, size))

# takes a date like 1990-01 (YYYY-MM) and converts it to Jan '90
date_formatter: Callable[[str], str] = lambda date: dt.strftime(dt.strptime(date, '%Y-%m'), "%b \'%y")
terabytes: Callable[[int], int] = lambda i: (i / 1024 ** 4)  # convert int to terabytes
xy_vals: list[tuple[str, int]] = [(date_formatter(month), terabytes(size)) for month, size in camera_data]

plt.rcParams.update({'font.size': 6})
plt.subplot(1, 2, 1)
plt.bar([x for x,y in xy_vals], [y for x,y in xy_vals])
plt.ylabel('Disk Usage (Terabytes)')
plt.xticks(rotation=60)
plt.title('12 Months')
plt.subplot(1, 2, 2)

cam_data_6mo_ago: list[tuple[str, int, str]] = [
    (date_formatter(month), terabytes(size), human_readable(size))
    for month, size in camera_data[-7:]
]  # the previous six months including the current month

pie_vals: list[int] = [size for _, size, _ in cam_data_6mo_ago]
pie_labels: list[str] = [f'{hr_size}\n{month}' for month, _, hr_size in cam_data_6mo_ago]

vals_and_labels : list[tuple[str, int]] = [(f'{hr_size}\n{month}', size) for month, size, hr_size in cam_data_6mo_ago]

plt.pie(x=pie_vals, labels=pie_labels)
plt.title('6 Months')
plt.suptitle(f"ZoneMinder Monthly Usage -- {dt.now().strftime('%Y-%m-%d')}")  # shows the current date
plt.savefig(save_fig_location, dpi=800)
plt.clf()
