import pandas as pd
import sqlite3
from os.path import isdir
from admintools import DiskMount, MyLogger
from zm_lib import disk_uuid, mount_point, zm_dir, camera_caches, db_file, log_file_name


logger = MyLogger(
    name='zm_db_path',
    level=10,
    to_console=False,
    to_file=log_file_name
).logger


con: sqlite3.Connection = sqlite3.connect(db_file)
df: pd.DataFrame = pd.read_sql('select * from zm_sizes', con)


save_to_db: bool = True
allow_unmount: bool = False
df['status']: None | str = None
df['path']: None | str = None

zm_backup_vol: DiskMount = DiskMount(uuid=disk_uuid)

try:
    found_mount_point: str = zm_backup_vol.find_mountpoint()
    mount_point: str = zm_backup_vol.mount_point
    logger.info(f'Drive is already mounted at {found_mount_point}')
except DiskMount.DiskMountError:
    # Trying to find the mountpoint of an unmounted disk will trow this exception
    zm_backup_vol.mount(mount_point)
    logger.info(f'Mounting drive to {mount_point}')

zm_backup_base: str = f'{mount_point}/zm_cache'

for index in df.index.tolist():
    date: str = df.at[index, 'date']

    zm_system_caches: list[str] = [f'{zm_dir}/{cache}/{date}' for cache in camera_caches]
    zm_backup_caches: list[str] = [f'{zm_backup_base}/{cache}/{date}' for cache in camera_caches]

    hit: bool = False
    
    for cache in zm_system_caches:
        if isdir(cache):
            hit = True
            df.at[index, 'status'] = 'on_system'
            df.at[index, 'path'] = zm_dir
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
    logger.debug(f'Unmounting {mount_point}')
else:
    logger.debug('unmount not allowed')

con.commit()
con.close()

logger.warning('Done\n\n')
