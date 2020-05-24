'''
This is a module that will be able to setup remote proccess to other devices over
a network, using ssh and scp.
'''

__Authour__ = 'HarryBurge'
__Date__ = '25/03/2020'
__Last_Updated_By__ = 'Harry Burge'
__Last_Updated__ = '24/05/2020'


# imports
import paramiko
from scp import SCPClient
from threading import Thread


# RemoteWorker
class RemoteWorker():
    '''
    Holds one indurvidual worker, and ficilitates remote code execution
    '''

    def __init__(self, server, port, user, password):
        '''
        params:-
            server : str : IP or name of server
            port : str : SSH port
            user : str : User to ssh into
            password : str : SSH password for that user
        '''

        # creates a ssh connection with the correct id
        temp = paramiko.SSHClient()
        temp.load_system_host_keys()
        temp.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        temp.connect(server, port, user, password)

        # saves ssh client connection
        self.ssh_client = temp

        # saves scp client connection
        self.scp_client = SCPClient(self.ssh_client.get_transport())


    def scp_put(self, files, remote_path=b'.', recursive=False, preserve_times=False):
        '''
        params:-
            files : str : Path to file wanting to be transfered
            remote_path : str : Path on remote worker to place it
            recursive : bool : True if file is folder and you want to copy os subdirectories
            preserve_times : bool : Look at scp doc, bit redundant in my use of this module
        returns:-
            None
        '''
        self.scp_client.put(files, remote_path, recursive, preserve_times)


    def scp_get(self, remote_path, local_path='', recursive=False, preserve_times=False):
        '''
        params:-
            remote_path : str : Path on remote worker to files wanting to be collected
            local_path : str : Where you want to store the returned file
            recursive : bool : True if file is folder and you want to copy os subdirectories
            preserve_times : bool : Look at scp doc, bit redundant in my use of this module
        returns:-
            None
        '''
        self.scp_client.get(remote_path, local_path, recursive, preserve_times)


    def ssh_command(self, command, results, index):
        '''
        params:-
            command : str : Ssh command to be executed on the remote worker
            results : [None|str, ...] : Byref for things to be able to return info from a thread
            index : int : Index assigned for its output in results
        returns:-
            None : returns result by changing results[index]
        '''
        self.ssh_client.invoke_shell()
        stdin, stdout, stderr = self.ssh_client.exec_command(command)
        results[index] = stdout.read().decode("utf-8")


# RemoteWorkerController
class RemoteWorkerController():
    '''
    Holds multiple RemoteWorkers and provides an easier way of running code on them
    '''

    def __init__(self, workers=[]):
        '''
        params:-
            workers : RemoteWorker : Workers to be controlled by this
        '''
        self.workers = workers


    def addWorker(self, server, port, user, password):
        '''
        params:-
            server : str : IP address of remote worker
            port : str : Port number of the ssh open port
            user : str : User to login to
            password : str : Password of the user
        returns:-
            None
        '''
        self.workers.append(RemoteWorker(server, port, user, password))


    def worker_ssh_command(self, worker_index, command, results, results_index):
        '''
        params:-
            worker_index : int : Index of worker in self.workers
            command : str : Command to be exectued on that worker
            results : [None|str, ...] : Byref for things to be able to return info from a thread
            results_index : int : Index assigned for its output in results
        return:-
            None : returns result via byref of results
        '''
        self.workers[worker_index].ssh_command(command, results, results_index)


    def all_workers_scp_put(self, files, remote_path=b'.', recursive=False, preserve_times=False):
        '''
        params:-
            files : str : Path to file wanting to be transfered
            remote_path : str : Where to place file on remote worker
            recursive : bool : True if file is folder and wanting to also put subdirectories
            preservive_times : bool : Look at SCP documentation, not being used for what I need it for
        returns:-
            None
        '''
        threads = []

        # For all workers do a put command for the file and do this multithreaded
        for index, i in enumerate(self.workers):
            threads.append(Thread(target=i.scp_put, args=(files, remote_path, recursive, preserve_times)))
            threads[index].start()

        # Make sure all threads have finished
        for i in threads:
            i.join()


    def all_workers_scp_get(self, remote_path, local_path='', recursive=False, preserve_times=False):
        '''
        params:-
            remote_path : str : Where to get file on remote worker
            local_path : str : Where to store file on local machine
            recursive : bool : True if file is folder and wanting to also put subdirectories
            preservive_times : bool : Look at SCP documentation, not being used for what I need it for
        returns:-
            None
        '''
        threads = []

        # For all workers do a get command for the file and do this multithreaded
        for index, i in enumerate(self.workers):
            threads.append(Thread(target=i.scp_get, args=(remote_path, local_path, recursive, preserve_times)))
            threads[index].start()

        # Make sure all threads have finished
        for i in threads:
            i.join()


    def all_workers_ssh_command(self, command):
        '''
        params:-
            command : str : Command for all workers to execute
        returns:-
            [str, ...] : Returns anything printed from the command executed
        '''
        threads = []
        results = [None] * len(self.workers)

        # For all workers do a command and do this multithreaded
        for index, i in enumerate(self.workers):
            threads.append(Thread(target=i.ssh_command, args=(command, results, index)))
            threads[index].start()

        # Make sure all threads have finished
        for i in threads:
            i.join()
        
        return results

    
    def compute_commands_file(self, files, commands, remote_path='/root', recursive=False, command_for_removal='rm -r '):
        '''
        Takes a file and a bunch of ssh commands and runs them on all workers until commands are completed
        params:-
            files : (str, ..) : paths to files to be transfered across
            commands : (str, ...) : ssh commands to be sent to the workers to run the things on the file
            recursive : bool : if file is folder then make true if you want all subdirectories
        returns:-
            [str, ...] : Results from all the commands ran, index will be the same as the commands
        '''
        # Adds needed files to all the workers
        for file_path in files:
            self.all_workers_scp_put(file_path, remote_path, recursive)

        results = [None] * len(commands)
        threads = [None] * len(self.workers)

        # Loops on commands
        for com_index, com in enumerate(commands):
            
            # Checks whether the command has actually been executed
            command_exe = False

            # Loops until command is put into a thread
            while command_exe == False:

                try:
                    found_index = threads.index(None)

                    # Creates new thread in slot avaliable
                    threads[found_index] = Thread(target=self.worker_ssh_command, args=(found_index, com, results, com_index))
                    threads[found_index].daemon = True
                    threads[found_index].start()

                    command_exe = True

                # If no space free in threads then loop until there is
                except ValueError:
                    pass

                # Check if threads finished if it has clear it for another thread to use
                for thread_index, thread in enumerate(threads):

                    if thread != None and not(thread.is_alive()):
                        threads[thread_index] = None

        # Check all threads are completed
        for thread in threads:
            if thread != None:
                thread.join()

        # Remove the files transferred to keep workers clean
        for file_path in files:
            self.all_workers_ssh_command('cd ' + remote_path + ' && ' + command_for_removal + file_path.split('/')[-1])

        return results



if __name__ == '__main__':
    raise RuntimeError('This is a module and cannot be ran')
