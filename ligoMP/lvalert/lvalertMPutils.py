description = "a module that holds methods and base classes for lvalert_listenMP functionality"
author = "reed.essick@ligo.org"

#---------------------------------------------------------------------------------------------------

from numpy import infty

import subprocess as sp

import time

#---------------------------------------------------------------------------------------------------

def sendEmail( recipients, body, subject ):
    """
    a wrapper for the commands that send emails.
    delegates to subprocess

    raises an error if returncode != 0
    """
    proc = sp.Popen(["mail", "-s", subject, " ".join(recipients)], stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE)
    out, err = proc.communicate(input=body)
    if proc.returncode: ### there was an issue
        raise RuntimeError('email failed to send\nstdout : %s\nstderr : %s'%(out, err))

#---------------------------------------------------------------------------------------------------

class SortedQueue(object):
    """
    an object representing a sorted Queue
    items are sorted by their expiration times (when they timeout)

    WARNING: we may want to replace this with the SortedContainers module's SortedList(WithKey?) 
        almost certainly has faster insertion than what you've written and comes with many convenient features already implemented
    """

    def __init__(self):
        self.__queue__ = []
        self.complete = 0

    def __len__(self):
        return len(self.__queue__)

    def __getitem__(self, ind):
        return self.__queue__[ind]

    def insert(self, newItem):
        """
        insert a newItem into the queue
        requires newItem to be a subclass of QueueItem and 
        always inserts newItem in the correct location to 
        preserve the queue's order

        WARNING: insertion is achieved via a direct iteration over the queue. 
        We may be able to speed this up with a clever data structure (besides a linked list)
        """
        if not isinstance(newItem, QueueItem):
            raise ValueError("SortedQueue *must* contain only QueueItems")

        for item in self.__queue__: ### iterate through queue and insert where appropriate
            if item.expiration > newItem.expiration:
                self.__queue__.insert( ind, newItem )
                break
        else:
            self.__queue__.append( newItem )
        self.complete += newItem.complete

    def pop(self, ind=0):
        """
        removes and returns the item stored at ind in the queue
        """
        item = self.__queue__.pop(ind)
        self.complete -= item.complete
        return item

    def clean(self):
        """
        remove all completed items from the queue
        """
        remove = [ind for ind, item in enumerate(self.__queue__) if item.complete] ### identify the items that are complete
        remove.reverse() ### start from the back so we don't mess up any indecies
        for ind in remove:
            queue.pop(ind) ### remove this item
        self.complete = 0

    def resort(self):
        """
        sorts all items in case there's been modifications
        Hopefully, this won't be needed but we provide it just in case
        """
        self.__queue__.sort(key=lambda item: item.expiration)

    def setComplete(self):
        """
        iterates over self.queue to determine the number of completed tasks

        this should NOT be necessary as long as queue is properly managed externally
        """
        self.complete = sum([item.complete for item in self.__queue__])
        
#-------------------------------------------------

class Task(object):
    """
    a task to be complted by a QueueItem
    this basic object manages execution via delegation to a functionHandle supplied when instantiated
    child classes may simply define their execution commands directly as part of the class definition

    functionHandle is called using this signature:
        self.functionHandle( verbose=verbose, *self.args, **self.kwargs )
    """
    name = "task"
    description = "a task"

    def __init__(self, timeout, functionHandle, *args, **kwargs ):

        self.timeout = timeout
        self.expiration = None ### we have to set this

        self.functionHandle = functionHandle

        self.args = args
        self.kwargs = kwargs

    def setExpiration(self, t0):
        """
        set expiration relative to start time provided (t0)
        """
        self.expiration = t0+self.timeout

    def hasExpired(self):
        """
        check whether this task has timed out
        """
        if self.expiration==None:
            raise ValueError("must call setExpiration before calling hasExpired!")
        return time.time() > self.expiration

    def execute(self, verbose=False):
        """
        perform associated function call
        """
        return self.functionHandle( verbose=verbose, *self.args, **self.kwargs )

class QueueItem(object):
    """
    an item for the sorted Queue
    each item contains a list of tasks that must be completed before the item is complete
    In this way, each follow-up process should get 1 item that models the entire behavior of that process
    """
    name = "item"
    description = "a series of connected tasks"

    def __init__(self, t0, tasks):

        self.t0 = t0
        self.tasks = []
        self.completedTasks = []

        if len(tasks): ### there is something to do
            self.add( tasks )
            self.complete = False
        else: ### there is nothing to do
            self.expiration = -infty ### nothing to do, so we are already expired
            self.complete = True

    def sortTasks(self):
        """
        sort (remaining) tasks by expiration
        """
        if self.tasks:
            self.tasks.sort(key=lambda l: l.expiration)
            self.expiration = self.tasks[0].expiration
        else:
            self.expiration = -infty

    def setExpiration(self, t0):
        '''
        updates the expiration of all tasks as well as of the QueueItem itself.
        '''
        for task in self.tasks:
            task.setExpiration(t0) ### update expiration of each task
        self.sortTasks() ### sorting tasks in the QueueItem. This automatically updates self.expiration

    def hasExpired(self):
        """
        check whether the next task has expired
        """
        return time.time() > self.expiration

    def execute(self, verbose=False):
        """
        execute the next task
        """
        while len(self.tasks):
            self.expiration = self.tasks[0].expiration
            if self.hasExpired():
                task = self.tasks.pop(0) ### extract this task
                task.execute( verbose=verbose ) ### perform this task
                if task.hasExpired(): ### check whether the task is actually done
                    self.completedTasks.append( task ) ### mark as completed
                else: ### task is NOT done, add it back in
                    self.add( [task] )
            else:
                break
        self.complete = len(self.tasks)==0 ### only complete when there are no remaining tasks

    def add(self, newTasks):
        """
        adds a new task to the stored list
        """
        if not hasattr( newTasks, "__iter__"):
            newTasks = [newTasks]
        for task in newTasks:
            if not issubclass(type(task), Task):
                raise ValueError("each element of tasks must be an instance of ligo.lvalert.lvalertMPutils.Task")
            task.setExpiration( self.t0 )
            self.tasks.append( task )
        self.sortTasks() ### ensure tasks are sorted

    def remove(self, taskName):
        """
        removes and returns the first instance of Task with a name matching taskName
        if taskName does not match any of the existing tasks, we raise a KeyError
        """
        for ind, task in enumerate(self.tasks):
            if task.name==taskName:
                return self.tasks.pop( ind )
            if len(self.tasks):
                self.expiration = self.tasks[0].expiration
            else:
                self.expiration = -np.infty
        else:
            raise KeyError('could not find a task with name=%s'%(taskName))
