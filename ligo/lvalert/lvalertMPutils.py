description = "a module that holds methods and base classes for lvalert_listenMP functionality"
author = "reed.essick@ligo.org"

#---------------------------------------------------------------------------------------------------

from numpy import infty

import time

#---------------------------------------------------------------------------------------------------

def printAlert( graceid, alert="blah" ):
    """
    an example action that we trigger off of an alert
    """
    print "%s : %s" % (graceid, alert)

def parseAlert( queue, queueByGraceID, alert, t0, config, timeout=5.0 ):
    """
    figures out what type of action needs to be taken and modifies SortedQueue as needed
    """
    graceid = alert['uid']

    ### generate the tasks needed
    task = Task( timeout, printAlert, description="print alert text", alert=alert )

    ### generate the Item which houses the tasks
    item = QueueItem( graceid, t0, [task], description="print the alert text" )

    ### add the item to the queue
    queue.insert( item )

    ### add the item to the queue for this specific graceID
    if not queueByGraceID.has_key(graceid):
        queueByGraceID[graceid] = SortedQueue()
    queueByGraceID[graceid].insert( item )

    return 0 ### return the number of *new* complete items that are now in the queue

#---------------------------------------------------------------------------------------------------

class SortedQueue(object):
    """
    an object representing a sorted Queue
    items are sorted by their expiration times (when they timeout)

    WARNING: we may want to replace this with the SortedContainers module's SortedList(WithKey?) 
        almost certainly has faster insertion than what you've written and comes with many convenient features already implemented
    """

    def __init__(self):
        self.queue = []

    def __len__(self):
        return len(self.queue)

    def __getitem__(self, ind):
        return self.queue[ind]

    def insert(self, newItem):
        if not isinstance(newItem, QueueItem):
            raise ValueError("SortedQueue *must* contain only QueueItems")

        for item in self.queue: ### iterate through queue and insert where appropriate
            if item.expiration > newItem.expiration:
                self.queue.insert( ind, newItem )
                break
        else:
            self.queue.append( newItem )

    def pop(self, ind):
        return self.queue.pop(ind)

#-------------------------------------------------

class Task(object):
    """
    a task to be complted by a QueueItem
    this basic object manages execution via delegation to a functionHandle supplied when instantiated
    child classes may simply define their execution commands directly as part of the class definition
    """

    def __init__(self, timeout, functionHandle, name="task", description="a task", *args, **kwargs ):

        self.timeout = timeout
        self.expiration = None ### we have to set this

        self.functionHandle = functionHandle

        self.name = name
        self.description = description

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

    def execute(self, graceid, gdb, verbose=False, annotate=False):
        """
        perform associated function call
        """
        return self.functionHandle( graceid, gdb, verbose=verbose, annotate=annotate, *self.args, **self.kwargs )

class QueueItem(object):
    """
    an item for the sorted Queue
    each item contains a list of tasks that must be completed before the item is complete
    In this way, each follow-up process should get 1 item that models the entire behavior of that process
    """

    def __init__(self, t0, tasks, description="a series of connected tasks"):

        self.description = description

        self.t0 = t0
        self.tasks = tasks
        self.completedTasks = []

        if len(tasks): ### there is something to do
            for task in self.tasks:
                if not isinstance(task, Task):
                    raise ValueError("each element of tasks must be an instance of ligo.lvalert.lvalertMPutils.Task")
                task.setExpiration( t0 )
            self.sortTasks() ### ensure tasks are sorted

            self.complete = False

        else: ### there is nothing to do
            self.expiration = -infty ### nothing to do, so we are already expired
            self.complete = True

    def sortTasks(self):
        """
        sort (remaining) tasks by expiration
        """
        self.tasks.sort(key=lambda l: l.expiration)
        self.expiration = self.tasks[0].expiration

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
                task.execute( self.graceid, self.gdb, verbose=verbose ) ### perform this task
                self.completedTasks.append( task ) ### mark as completed
            else:
                break
        self.complete = len(tasks)==0 ### only complete when there are no remaining tasks

#---------------------------------------------------------------------------------------------------

