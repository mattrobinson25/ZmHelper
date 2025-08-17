from os import listdir, mkdir
from os.path import isdir
from datetime import datetime as dt, timedelta as td
from admintools import DiskMount, byte_sizer, prune_log
from threading import Thread
import pickle
import subprocess
from time import sleep
import pandas as pd
import sqlite3
from zm_lib import (
    disk_uuid, log_file_name, mount_point, zm_size_db, keep_days, zm_dir, save_dir, logger, ZmHelper,
    delete_days, max_threads, allow_delete, allow_move, date_fmt, allow_unmount, camera_caches
)


def lock_handler(state: bool) -> None:
    with open(lock_file, 'wb') as file:
        logger.debug('Unlocking lock file')
        pickle.dump({'state': state}, file)


zm_helper: ZmHelper = ZmHelper()

# Check to see if program is locked
lock_file: str = f'{zm_dir}/.zm_move.lock'
try:
    with open(lock_file, 'rb') as file:
        lock_state_obj: dict[str, bool] = pickle.load(file)
except FileNotFoundError:
    logger.warning('Lock file not found.')
    lock_state_obj: dict[str, bool] = {'state': False}

is_locked: bool = lock_state_obj['state']

if is_locked:
    # Exit program if already locked.
    # Two instances of this program may not run at the same time.
    logger.error(f'Program is already running. Wait for finish or delete lock file {lock_file}\n')
    exit()
else:
    # Program is not locked. Lock now.
    logger.warning(' Beginning Backup '.center(80, '#'))
    logger.debug('Locking program now.')
    lock_handler(True)
 
logger.debug('Creating DiskMount instance')

try:
    backup_vol: DiskMount = DiskMount(uuid=disk_uuid)
except subprocess.CalledProcessError as e:
    logger.critical(e)
    logger.critical(f'Backup disk failed to mount. Perhaps it is disconnected.')
    lock_handler(False)
    exit()

try:
    backup_vol.find_mountpoint()  # finding mount point checks to see if disk is already mounted!

    if backup_vol.mount_point == mount_point:
        logger.warning('Backup disk is already mounted properly')
    else:
        logger.warning(f'Backup disk is already mounted at {backup_vol.mount_point}. Attempting to unmount now.')
        try:
            backup_vol.unmount()
            logger.info('Successfully unmounted')
        except DiskMount.DiskMountError:
            logger.critical('Backup disk could not be acquired.')
            logger.critical(backup_vol)
            exit()
except DiskMount.DiskMountError:
    logger.debug('Disk is not already mounted')


if not backup_vol.is_mounted:
    backup_vol.mount(mount_point=mount_point)


if backup_vol.is_mounted:
    logger.warning(backup_vol)  # calls __repr__() and shows some useful information
    logger.warning(f'Backup disk is at {backup_vol.disk_usage()}%')
else:
    logger.error('Backup disk was not mounted properly. Exiting now.')
    exit()

start: dt = dt.now()

if not isdir(save_dir):
    logger.warning(f'{save_dir} does not exist. Creating now.')
    mkdir(save_dir)

# Delete old saves in save_dir (older than delete_days)
time_threshold: dt = dt.now() - td(days=delete_days)
if allow_delete:
    logger.info(f"Searching {save_dir} for save_dir older than {time_threshold.strftime('%Y-%m-%d')} to delete.")


with sqlite3.connect(zm_size_db) as conn:
    df = pd.read_sql('select * from zm_sizes', conn)

delete_size: int = 0

if allow_delete:
    for cache in listdir(save_dir):
        cache_dir = f'{save_dir}/{cache}'

        for date_dir in listdir(cache_dir):
            try:
                date_dir_parsed: dt = dt.strptime(date_dir, '%Y-%m-%d')
            except ValueError:
                logger.error(f'Invalid date dir -- {date_dir} in {cache_dir}')
                continue

            if date_dir_parsed < time_threshold:
                target: str = f'{cache_dir}/{date_dir}'
                size: int = df[df.date == date_dir][cache].item()
                delete_size += size

                thread: Thread = Thread(
                    target=zm_helper.delete_worker,
                    args=(target, size)
                )
                zm_helper.delete_threads.append(thread)


disk_availability_human_readable: str = byte_sizer(backup_vol.disk_available())  # Space available on partition
disk_used_start: int = backup_vol.disk_used()  # How much space is currently being used on partition
disk_usage_start: int = backup_vol.disk_usage()  # Same as above - but as a percentage
delete_size_human_readable: str = byte_sizer(delete_size)

if allow_delete:
    logger.info(f'''        
                   Delete Job
        Available space: {disk_availability_human_readable}
             Disk Usage: {disk_usage_start}%
            Delete Size: {delete_size_human_readable}
            Num threads: {len(zm_helper.delete_threads)}
        ''')

    [task.start() for task in zm_helper.delete_threads]
    [task.join() for task in zm_helper.delete_threads]
else:
    logger.info('Deletion disabled')


# Begin move jobs
logger.info('Finished delete threads. Beginning move threads now.')
limit_reached: bool = False
time_threshold: dt = dt.now() - td(days=keep_days)
time_threshold_formatted: str = dt.strftime(time_threshold, date_fmt)  # Date formatted as YYYY-MM-DD
backup_size: int = 0

logger.info(f'Searching for directories {zm_dir} older than {time_threshold_formatted} to archive')


if allow_move:
    # Each camera has its own data cache
    for cache in camera_caches:
        # Each cache has directories named by the date they were created
        dates_cache: list[str] = listdir(f'{zm_dir}/{cache}')

        for date_dir in dates_cache:
            if limit_reached:
                continue
            else:
                try:
                    cache_date_parsed: dt = dt.strptime(date_dir, date_fmt)  # Convert dir name into datetime object
                except ValueError:
                    logger.warning(f'Invalid date folder found {zm_dir}/{cache}/{date_dir}. This should be deleted!')
                    continue

                if cache_date_parsed < time_threshold:
                    source: str = f'{zm_dir}/{cache}/{date_dir}'
                    destination: str = f'{save_dir}/{cache}/{date_dir}'

                    if len(zm_helper.archive_threads) < max_threads:  # Put a limit on how many jobs can be done per day
                        logger.debug(f'Found copy path {source}')
                        size: int = df[df.date == date_dir][cache].item()
                        backup_size += size

                        # archive_worker and move_worker will have the same args
                        thread: Thread = Thread(
                            target=zm_helper.archive_worker,
                            args=(source, destination, size, cache, date_dir, 'bztar')
                        )
                        zm_helper.archive_threads.append(thread)
                    else:
                        logger.warning(f'Maximum of {max_threads} move jobs reached for the day.')
                        limit_reached: bool = True


disk_availability_end: int = backup_vol.disk_available()
backup_size_human_readable: str = byte_sizer(backup_size)
disk_availability_human_readable: str = byte_sizer(disk_availability_end)

if allow_move:
    logger.info(f'''
                     Move Job
            Backup size: {backup_size_human_readable}
        Available space: {disk_availability_human_readable}
             Disk Usage: {backup_vol.disk_usage()}%
            Num threads: {len(zm_helper.archive_threads)}
            Max threads: {max_threads}
        ''')

    # check to see if the backup disk has enough space to handle the backup jobs
    if disk_availability_end > backup_size:
        status: str = 'Success'
        # Begin threads
        [task.start() for task in zm_helper.archive_threads]
        [task.join() for task in zm_helper.archive_threads]  # Program waits here for all threads to complete
    else:
        logger.error('The backup disk does not have enough space for the current set of jobs! Skipping!')
        status: str = 'Failure'
else:
    logger.warning('Move to backup is not allowed. Skipping!')
    status: str = 'Success'

if allow_unmount:
    try:
        backup_vol.unmount()
        logger.info('Successfully unmounted.')
        unmount_success: bool = True
    except subprocess.CalledProcessError as e:
        logger.error(e)
        logger.error(e.stderr)
        unmount_success: bool = False

    if not unmount_success:
        logger.error('Unmount failed. Trying again in 30 minutes.')
        sleep(1800)  # 30 minutes

        try:
            backup_vol.unmount()
            logger.info('Successfully unmounted.')
        except subprocess.CalledProcessError as e:
            logger.error(e)
            logger.error(e.stderr)
            logger.error('Unmount failed again. Skipping')
else:
    logger.warning('Unmount not allowed. Skipping unmount.')


lock_handler(False)  # Program finished. Unlock to allow new instances in the future.
disk_used_end: int = backup_vol.disk_used()  # hom much disk space is being used currently
disk_usage_end: int = backup_vol.disk_usage()  # percentage of how much disk space is being used currently
disk_size: int = backup_vol.disk_size()  # total size of disk partition
disk_change: int = disk_used_end - disk_used_start  # how much data was added to disk once finished
disk_usage_pcent: int = disk_usage_end - disk_usage_start

logger.debug(f'disk_used_start : {disk_used_start}')
logger.debug(f'disk_used_end : {disk_used_end}')
logger.debug(f'{disk_used_end} - {disk_used_start} = {disk_used_end - disk_used_start}')

sign: str = ''
if disk_change > 0:
    sign: str = '+'
    
if -1 > disk_usage_pcent < 1:
    disk_usage_pcent: int = 1
    sign: str = f'< {sign}'

logger.warning(f'''
            Status: {status.upper()}
          Run time: {dt.now() - start}  
        Backup Job: {backup_size_human_readable}
        Delete Job: {delete_size_human_readable}
    Change on Disk: {sign}{byte_sizer(disk_change)} ({sign}{disk_usage_pcent}%)
    
    {byte_sizer(backup_vol.disk_available())} remaining on backup disk.
    Backup disk usage is at {disk_usage_end}%.
''')

if disk_usage_end >= 90:
    message: str = f'Warning! ZM backup cache is at {disk_usage_end}%'
    date_and_time: str = dt.now().strftime('%c')
    full_message: str = f"{date_and_time} -- {message}\n"

    with open('/etc/motd', 'r') as file:
        text: str = file.read()

    if message[:-3] not in text:
        with open('/etc/motd', 'a') as file:
            file.write(full_message)
    else:
        with open('/etc/motd', 'r') as file:
            text_list: list[tuple[int, str]] = list(enumerate(file.readlines()))

        for index, line in text_list:
            if message[:-3] in line:
                text_list[index] = (index, full_message)

        with open('/etc/motd', 'w') as file:
            new_lines: list[str] = [line for _, line in text_list]
            file.writelines(new_lines)

logger.warning('Done!\n\n')
prune_log(log_file_name, length=5000)
