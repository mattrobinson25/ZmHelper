from os import getpid, listdir, makedirs
from os.path import islink, isdir
from datetime import datetime as dt
from threading import BoundedSemaphore
from admintools import MyLogger, byte_sizer
from shutil import copytree, rmtree, make_archive
from threading import Thread


# Zm-Move. user defined vars
working_dir : str = '/nfs_share/matt_desktop/server_scripts/zm_helper/'
semaphore : BoundedSemaphore = BoundedSemaphore(5)  # How many simultaneous copy jobs to allow at once
disk_uuid : str = '244815e3-6ef8-450b-b12c-6bcd1df08fa1'  # UUID of backup disk. $ blkid -o value -s UUID /dev/sdxx
log_file_name : str = '/var/log/zm_move.log'
mount_point : str = '/mnt/7'
zm_size_db : str = f'{working_dir}/zm_size.db'
keep_days : int = 90     # How long to keep videos on system before moving to backup
delete_days : int = 180  # How long to keep videos on backup before permanently deleting
max_threads : int = 30   # Max number of jobs per day
pid : int = getpid()
zm_dir : str = '/var/cache/zoneminder/events'
save_dir : str = f'{mount_point}/zm_cache'
camera_caches : list[str] = [
    directory for directory in listdir(zm_dir)
    if islink(f'{zm_dir}/{directory}')
]

# Main features. Set to False for testing purposes
allow_delete : bool = True    # Turn on/off delete feature
allow_move : bool = True      # Turn on/off move-to-backup feature
allow_unmount : bool = False  # Allow the backup disk to be unmounted at the end of the program

# Zm_Size_db
date_fmt : str = '%Y-%m-%d'
db_file : str = f'{working_dir}/zm_size.db'
today_date : str = dt.strftime(dt.now(), '%Y-%m-%d')  # YYYY-MM-DD


logger: MyLogger = MyLogger(
    name='zm_mover',
    to_file='/var/log/zm_move.log',
    to_console=False,
    level=20
).logger


class ZmHelper:
    def __init__(self):
        self.archive_counter : int = 0
        self.move_counter : int = 0
        self.delete_counter : int = 0
        self.archive_threads : list[Thread] = []
        self.move_threads : list[Thread] = []
        self.delete_threads : list[Thread] = []

    def move_worker(self, move_source: str, move_destination: str, move_size: int, move_cache_name: str) -> None:
        """Copies the source to the destination, then deletes the source."""
        with semaphore:
            start : dt = dt.now()
            human_readable_size : str = byte_sizer(move_size)

            if isdir(move_destination):
                logger.warning(f'{move_source} already exists in {move_destination}. '
                               f'Replacing now! -- {human_readable_size}')
            else:
                logger.info(f'Creating {move_destination}. Beginning backup. {human_readable_size}')
                makedirs(move_destination)

            copytree(src=move_source, dst=move_destination, dirs_exist_ok=True)

            if allow_delete:  # deletion is optional
                rmtree(move_source)

            self.move_counter += 1

            logger.info(f'''
                       Cache: {move_cache_name.upper()}
                      Source: {move_source}
                     Job num: {self.move_counter} of {len(self.move_threads)}
                        Size: {human_readable_size}
                 Destination: {move_destination}
                    Run time: {dt.now() - start}
                ''')

    def archive_worker(self, archive_source: str, archive_destination: str, archive_size: int,
                       archive_cache_name: str, archive_date: str, compression_type: str = 'bztar') -> None:
        """Archives (with compression) the source to the destination, then deletes the source."""
        with semaphore:
            start : dt = dt.now()

            if not isdir(archive_destination):
                logger.debug(f'{archive_destination} does not exist. Creating now.')
                makedirs(archive_destination)

            human_readable_size : str = byte_sizer(archive_size)
            logger.info(f'Beginning backup now {archive_destination} -- {human_readable_size}')

            if not isdir(archive_destination):
                logger.info(f'Creating {archive_destination}')
                makedirs(archive_destination)

            make_archive(
                base_name=f'{archive_destination}/{archive_date}_{archive_cache_name}',
                root_dir=archive_source,
                base_dir=archive_source,
                format=compression_type
            )

            if allow_delete:
                rmtree(archive_source)

            self.archive_counter += 1

            logger.info(f'''
                       Cache: {archive_cache_name.upper()}
                      Source: {archive_source}
                     Job num: {self.archive_counter} of {len(self.archive_threads)}
                        Size: {human_readable_size}
                 Destination: {archive_destination}
                    Run time: {dt.now() - start}
                ''')

    def delete_worker(self, del_path, del_size) -> None:
        """Only deletes the source."""
        with semaphore:
            human_readable_size = byte_sizer(del_size)
            rmtree(del_path)
            logger.info(f'''
                Finished deleting {del_path} - {human_readable_size}
                ''')
