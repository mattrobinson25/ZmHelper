import pandas as pd
import sqlite3
from os.path import isdir
from admintools import DiskMount, MyLogger
from os import listdir
from os.path import islink

logger: MyLogger = MyLogger(
    name='zm_db_path',
    level=10,
    to_console=False,
    to_file=False
).logger

con: sqlite3 = sqlite3.connect('/nfs_share/matt_desktop/server_scripts/zm_helper/zm_size.db')
df: pd = pd.read_sql('select * from zm_sizes', con)

disk_uuid: str = "244815e3-6ef8-450b-b12c-6bcd1df08fa1"
disk_mount_point: str = '/mnt/7'
zm_system_base: str = '/var/cache/zoneminder/events'
zm_caches: list[str] = [directory for directory in listdir(zm_system_base) if islink(f'{zm_system_base}/{directory}')]
save_to_db: bool = True
allow_unmount: bool = True
df['status']: None | str = None
df['path']: None | str = None

zm_backup_vol: DiskMount = DiskMount(uuid=disk_uuid)

try:
    mountpoint = zm_backup_vol.find_mountpoint()
    disk_mount_point: str = zm_backup_vol.mount_point
    logger.info(f'Drive is already mounted at {mountpoint}')
except DiskMount.DiskMountError:
    # Trying to find the mountpoint of an unmounted disk will trow this exception
    zm_backup_vol.mount(disk_mount_point)
    logger.info(f'Mounting drive to {disk_mount_point}')

zm_backup_base: str = f'{disk_mount_point}/zm_cache'

for index in df.index.tolist():
    date: str = df.at[index, 'date']

    zm_system_caches: list[str] = [f'{zm_system_base}/{cache}/{date}' for cache in zm_caches]
    zm_backup_caches: list[str] = [f'{zm_backup_base}/{cache}/{date}' for cache in zm_caches]

    hit: bool = False
    
    for cache in zm_system_caches:
        if isdir(cache):
            hit = True
            df.at[index, 'status'] = 'on_system'
            df.at[index, 'path'] = zm_system_base
            break

    for cache in zm_backup_caches:
        if isdir(cache):
            hit = True
            df.at[index, 'status'] = 'on_backup'
            df.at[index, 'path'] = zm_backup_base
            break

    # no matches were made
    if not hit:
        df.at[index, 'status'] = 'deleted'
        df.at[index, 'path'] = None


if save_to_db:
    df.to_sql(
        name='zm_sizes',
        con=con,
        if_exists='replace',
        index=False
    )
else:
    logger.info(df)

if allow_unmount:
    zm_backup_vol.unmount()
    logger.debug(f'Unmounting {disk_mount_point}')
else:
    logger.debug('unmount not allowed')


con.commit()
con.close()
