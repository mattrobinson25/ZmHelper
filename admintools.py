from os import getcwd, walk
from os.path import ismount, getsize, isdir, isfile
import subprocess
from math import isnan
import logging
from sys import stdout, argv

class Servers:
    """
    Turn SSH servers into python objects.
    With this class you can ping, run commands, send files, receive files, mount nfs to client, and check disks. Server
    objects will also have attributes that you can script custom logic to.

    To create a server object you will need ip-address, port, and username for the host.
    You can also host an optional nfs path.

    example_server = Servers(ip='192.168.1.115', user='mrobinson', port=22, nfs_path='/nfs_share/')

    First, check to see that the server is up
    example_server.ping()"""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.is_mounted:
            self.nfs_unmount()

    def __init__(self,
                 ip : str,
                 user : str,
                 port : int=22,
                 nfs_path: str | None=None,
                 check_connection : bool=True):

        self.subprocess_exceptions = (subprocess.CalledProcessError, subprocess.SubprocessError)
        self.ip : str = ip              # IP Address of remote server
        self.port : int = port		  # ssh port (default 22)
        self.user : str = user  		  # username on remote server
        self.nfs_path : str = nfs_path  # Path to nfs directory on server (default None)
        self.mount_point = None   # Path to nfs mount point on client
        self.is_mounted : bool = False   # Is nfs directory currently mounted on client?
        self.is_alive : bool = False     # Will ping server to see if there is a connection

        if check_connection:
            """Test for a stable network connection. If it passes, self.is_alive will be set to True"""
            self.ping(5)

    def __repr__(self):
        return f'''
        REMOTE SERVER
            IP Address: {self.ip}
              SSH Port: {self.port}
                  User: {self.user}
              NFS Path: {self.nfs_path}
              Is Alive: {self.is_alive}
          
        LOCAL CLIENT
                 Mount: {self.mount_point}
               Mounted: {self.is_mounted}
        '''

    def run(self, cmd: str):
        cmd_list : list[str] = cmd.split()
        destination : str = f'{self.user}@{self.ip}'
        ssh_arg : str = f'-p {self.port}'
        args_list : list[str] = ['ssh', ssh_arg, destination]

        args_list.extend(cmd_list)  # Concat args_list with cmd_list

        try:
            proc : subprocess.CompletedProcess[str] = subprocess.run(
                args=args_list,
                check=True,
                text=True,
                capture_output=True
            )
            return proc

        except self.subprocess_exceptions as error:
            print('\nAn error occurred.')
            print(error)
            return error

    def send_file(self, client_path: str, server_path: str | None=None):
        if server_path is None:
            server_path : str = f'/home/{self.user}'

        target : str = client_path
        destination : str = f"{self.user}@{self.ip}:/{server_path}"
        ssh_arg : str = f"ssh -p {self.port}"

        try:
            proc : subprocess.CompletedProcess[bytes] = subprocess.run(
                args=['rsync', '-avrP', target, '-e', ssh_arg,  destination],
                check=True
            )
            return proc
        except self.subprocess_exceptions as error:
            print('An error occurred.')
            print(error)
            return error

    def receive_file(self, server_path: str, client_path: str=getcwd()):
        target : str = f'{self.user}@{self.ip}:/{server_path}'
        destination : str = client_path
        ssh_arg : str = f"ssh -p {self.port}"

        try:
            proc : subprocess.CompletedProcess[bytes] = subprocess.run(
                args=['rsync', '-avrP', '-e', ssh_arg, target, destination],
                check=True
            )
            return proc

        except self.subprocess_exceptions as error:
            print('An error occurred.')
            print(error)
            return error

    def nfs_mount(self, mount_path: str):
        if ismount(mount_path):
            print(f'Directory {mount_path} is already mounted.')
        else:
            target : str = f'{self.ip}:{self.nfs_path}'
            destination : str = mount_path

            if self.nfs_path is None:
                print('NFS has not been set up for this server.')
            else:
                if self.is_mounted is False:
                    try:
                        proc : subprocess.CompletedProcess[bytes] = subprocess.run(
                            args=['sudo', 'mount', target, destination],
                            check=True
                        )
                        self.mount_point : str = mount_path
                        self.is_mounted : bool = True
                        return proc
                    except self.subprocess_exceptions as error:
                        print('An error occurred.')
                        print(error)
                        return error

                else:
                    print('NFS is already being used for this server. Unmount first.')
                    print(self.mount_point)

    def nfs_unmount(self):
        try:
            proc : subprocess.CompletedProcess[bytes] = subprocess.run(
                args=['sudo', 'umount', self.mount_point],
                check=True
            )
            self.mount_point : None | str = None
            self.is_mounted : bool = False
            return proc
        except self.subprocess_exceptions as error:
            print('An error occurred.')
            print(error)
            return error

    def ping(self, packets: int=5):
        try:
            proc : subprocess.CompletedProcess[bytes] = subprocess.run(
                args=['ping', '-c', str(packets), self.ip],
                check=True
            )
            self.is_alive : bool = True

            print('''
            
            The server is up.
            
            ''')
            return proc
        except self.subprocess_exceptions as error:
            print(f'Could not ping {self.ip} - The server is unreachable.')
            self.is_alive : bool = False
            return error

    def disk_free(self, device: str, ssh_arg: str | None=None, port: int=22) -> int:
        target : str = f'{self.user}@{self.ip}'

        if ssh_arg:
            ssh_arg : str = f'-o {ssh_arg}'
            try:
                cmd_out : str = subprocess.run(
                    args=['ssh', f'-p {port}', ssh_arg, target, 'df', device, '--output=pcent'],
                    text=True,
                    capture_output=True,
                    check=True
                ).stdout

                usage_int : int = int(
                    cmd_out.split()[1][:-1]
                )

                print(f'Device {device} usage at {usage_int}%')
                return usage_int
            except self.subprocess_exceptions as error:
                print('An error occurred.')
                print(error)
                return error

        else:
            try:
                cmd_out : str = subprocess.run(
                    args=['ssh', f'-p {port}', target, 'df', device, '--output=pcent'],
                    text=True,
                    capture_output=True,
                    check=True
                ).stdout

                usage_int : int = int(
                    cmd_out.split()[1][:-1]
                )

                print(f'Device {device} usage at {usage_int}%')
                return usage_int

            except self.subprocess_exceptions as error:
                print('An error occurred.')
                print(error)
                return error


class DiskMount:
    """
    Turn mountable disks into python objects.
    With this script you can mount, unmount, and check disk availability and usage. Disk objects will also have
    attributes that you can write custom logic around.
    """

    class DiskMountError(Exception):
        def __init__(self, message='Disk mount failed'):
            self.message : str = message
            super().__init__(self.message)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.is_mounted:
            self.unmount()

    def __init__(self, uuid: str):
        self.uuid : str = uuid
        self.is_mounted : bool = False
        self.mount_point : None = None

        self.source : str = subprocess.run(
            args=['blkid', '-o', 'value', '-U', self.uuid],
            text=True,
            capture_output=True,
            check=True
        ).stdout[:-1]

    def __repr__(self):
        return f'''
             Drive: {self.source}
              UUID: {self.uuid}
           Mounted: {self.is_mounted}
        Mountpoint: {self.mount_point}
        '''

    def mount(self, mount_point:str , options:str | None=None):
        if ismount(mount_point):
            self.is_mounted : bool = False
            self.mount_point : None = None

            raise self.DiskMountError(message=f'Mount point {mount_point} is already taken')
        else:
            if options:
                args : list[str] = ['sudo', 'mount', '-o', options, '-U', self.uuid, mount_point]
            else:
                args : list[str] = ['sudo', 'mount', '-U', self.uuid, mount_point]

            proc_return : subprocess.CompletedProcess[str] = subprocess.run(
                args=args,
                text=True,
                capture_output=True,
                check=True
            )
            self.is_mounted : bool = True
            self.mount_point : None | str = mount_point

            return proc_return

    def unmount(self):
        proc_return : subprocess.CompletedProcess[str] = subprocess.run(
            args=['sudo', 'umount', self.mount_point],
            text=True,
            capture_output=True,
            check=True
        )
        self.mount_point : None | str = None
        self.is_mounted : bool = False
        return proc_return

    def disk_usage(self) -> int:
        """Percent used on disk partition"""
        if self.is_mounted:
            df_percent_free : str = subprocess.run(
                args=['df', self.source, '--output=pcent'],
                text=True,
                capture_output=True,
                check=True
            ).stdout

            df_percent_free_int : int = int(df_percent_free.split()[-1][:-1])
            return df_percent_free_int
        else:
            raise self.DiskMountError(
                message='Disk must be mounted before calling disk_usage()'
            )

    def disk_size(self) -> int:
        """Total size of formatted space on disk partition"""
        if self.is_mounted:
            df_total_size : str = subprocess.run(
                args=['df', self.source, '--output=size'],
                text=True,
                capture_output=True,
                check=True
            ).stdout

            df_total_size : int = int(df_total_size.split()[-1][:-1])
            return df_total_size * 1024

        else:
            raise self.DiskMountError(
                message='Disk must be mounted before calling disk_size().'
            )

    def disk_available(self) -> int:
        """Show how much space is available on disk partition"""
        if self.is_mounted:
            avail : str = subprocess.run(
                args=['df', self.source, '--output=avail'],
                text=True,
                capture_output=True,
                check=True
            ).stdout

            avail_int : int = int(avail.split()[-1])  # Gives back how many 1k block sizes
            return avail_int * 1024
        else:
            raise self.DiskMountError(
                message='Disk must be mounted before calling disk_available()'
            )

    def disk_used(self) -> int:
        """Show how much space is used on disk partition"""
        if self.is_mounted:
            used : str = subprocess.run(
                args=['df', self.source, '--output=used'],
                text=True,
                capture_output=True,
                check=True
            ).stdout
            
            used_int : int = int(used.split()[-1])
            return used_int * 1024
        else:
            raise self.DiskMountError(
                message='Disk must be mounted before calling disk_used()'
            )

    def find_mountpoint(self) -> str:
        """Checks to see if disk partition is already mounted"""
        mount_point : None = None

        mount_output : list[str] = subprocess.run(
            args=['mount'],
            capture_output=True,
            text=True
        ).stdout.split('\n')

        for line in mount_output:
            if self.source in line:
                line_elements : list[str] = line.split()
                mount_point : str = line_elements[2]
                break

        if mount_point:
            self.mount_point : str = mount_point
            self.is_mounted : bool = True
            return mount_point
        else:
            self.mount_point : None | str = None
            self.is_mounted : bool = False
            raise self.DiskMountError(
                message='Disk must be mounted before calling find_mountpoint().'
            )


def byte_sizer(file_size: int, round_digit: int=2) -> str:
    """Get a human-readable string based on very large integers representing the size of a file or directory. Numbers
    like 1024 will be represented as 1 Mb"""

    kilobyte : int = 10**3
    megabyte : int = 10**6
    gigabyte : int = 10**9
    terabyte : int = 10**12
    petabyte : int = 10**15

    if isnan(file_size):
        file_size : int = 0

    if file_size < kilobyte:
        exponent : int = 0
        byte_type : str = 'Bytes'
    elif kilobyte <= file_size < megabyte:
        exponent : int = 1
        byte_type : str = 'Kb'
    elif megabyte <= file_size < gigabyte:
        exponent : int = 2
        byte_type : str = 'Mb'
    elif gigabyte <= file_size < terabyte:
        exponent : int = 3
        byte_type : str = 'Gb'
    elif terabyte <= file_size < petabyte:
        exponent : int = 4
        byte_type : str = 'Tb'
    else:
        exponent : int = 5
        byte_type : str = 'Pb'

    byte_size : float = round(
        number=(file_size / 1024 ** exponent),
        ndigits=round_digit
    )
    return f'{byte_size} {byte_type}'  # human-readable size like "3.4 Gb"


def get_dir_size(path: str) -> int:
    """Crawl through a directory and get the size of everything it contains. This will return a very large integer which
    can be turned into a human-readable string using the function above."""

    if isdir(path):
        size_counter : int = 0

        for root, dirs, files in walk(path):
            for file in files:
                size_counter += getsize(f'{root}/{file}')

        return size_counter
    else:
        raise NotADirectoryError


def rsync(src: str, dest: str, args: str='-ar') -> subprocess.CompletedProcess[str]:
    """A simple python wrapper for rsync"""
    
    return subprocess.run(
        args=['rsync', args, src, dest],
        text=True,
        capture_output=True
    )


def prune_log(file: str, length: int=10000) -> None:
    """Take excessively long logs and overwrite them with only the last 10,000 lines."""

    length : int = abs(length)  # enforce positive int
    if isfile(file):
        with open(file, 'r') as fh:
            text : list[str] = fh.readlines()

        pruned_text : list[str] = text[(length * -1):]  # convert to negative int

        with open(file, 'w') as fh:
            fh.writelines(pruned_text)
    else:
        raise FileNotFoundError


def os_release() -> dict[str, str]:
    """Reads the /etc/os-release file and returns a python dict based on your distribution's information. This allows
    scripts to take advantage of custom logic based on what distribution the script is being executed on."""

    with open('/etc/os-release', 'r') as fh:
        text: list[str] = fh.read().replace('\"', '').split('\n')

    release: dict[str, str] = {}

    for element in text:
        if (
                element
                and '=' in element
                and not element.startswith('#')
        ):
            items: list[str] = element.split('=')
            k: str = items[0]
            v: str = items[1]
            release[k] = v

    return release


class MyLogger(logging.Logger):
    """A simplified wrapper for the python logger module with some common pre-configurations. This makes it easy to
    initialize a simple logger in a python file that can be imported by other scripts using the same logging attributes"""

    def __init__(self,
                 name : str = argv[0],
                 level : int = logging.INFO,
                 fmt : str = '%(asctime)s: %(message)s',
                 to_file : str | bool = False,   # path to file (does not need to exist), or False to ignore file
                 to_console : bool = True):      # True to output to console, or False to ignore console

        super().__init__(name, level)
        self.name : str = name
        self.level : int = level
        self.to_file : str | bool = to_file
        self.to_console : bool = to_console

        self.logger : logging.Logger = logging.getLogger(name)
        self.fmt : logging.Formatter = logging.Formatter(fmt)

        # Logger levels
        self.NOTSET : int = 0
        self.DEBUG : int = 10
        self.INFO : int = 20
        self.WARNING : int = 30
        self.ERROR : int = 40
        self.CRITICAL : int = 50

        if to_console:
            self.console_stream : logging.StreamHandler = logging.StreamHandler(stdout)
            self.logger.addHandler(self.console_stream)
        if to_file:
            self.file_stream : logging.FileHandler = logging.FileHandler(to_file)
            self.logger.addHandler(self.file_stream)
            self.file_stream.setFormatter(self.fmt)

        self.logger.setLevel(level)
