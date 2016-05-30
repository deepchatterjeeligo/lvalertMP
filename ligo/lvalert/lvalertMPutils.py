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

def parseAlert( queue, queueByGraceID, alert, t0, config ):
    """
    figures out what type of action needs to be taken and modifies SortedQueue as needed
    """
    graceid = alert['uid']

    ### generate the tasks needed
    ### we print the alert twice to ensure the QueueItem works as expected with multiple Tasks
    taskA = Task(  5.0, printAlert, alert=alert )
    taskB = Task( 10.0, printAlert, alert=alert )

    ### generate the Item which houses the tasks
    item = QueueItem( graceid, t0, [taskA, taskB] )

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

        for item in self.queue: ### iterate through queue and insert where appropriate
            if item.expiration > newItem.expiration:
                self.queue.insert( ind, newItem )
                break
        else:
            self.queue.append( newItem )

    def pop(self, ind=0):
        """
        removes and returns the item stored at ind in the queue
        """
        return self.queue.pop(ind)

    def clean(self):
        """
        remove all completed items from the queue
        """
        remove = [ind for ind, item in enumerate(self.queue) if item.complete] ### identify the items that are complete
        remove.reverse() ### start from the back so we don't mess up any indecies
        for ind in remove:
            queue.pop(ind) ### remove this item

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
                self.completedTasks.append( task ) ### mark as completed
            else:
                break
        self.complete = len(tasks)==0 ### only complete when there are no remaining tasks

    def add(self, newTasks):
        """
        adds a new task to the stored list
        """
        if not hasattr( newTasks, "__iter__"):
            newTasks = [newTasks]
        for task in newTasks:
            if not issubclass(task, Task):
                raise ValueError("each element of tasks must be an instance of ligo.lvalert.lvalertMPutils.Task")
            task.setEpiration( t0 )
            self.tasks.append( newTask )
        self.sortTasks() ### ensure tasks are sorted

    def remove(self, taskName):
        """
        removes and returns the first instance of Task with a name matching taskName
        if taskName does not match any of the existing tasks, we raise a KeyError
        """
        for ind, task in enumerate(self.tasks):
            if task.name==taskName:
                return self.tasks.pop( ind )
        else:
            raise KeyError('could not find a task with name=%s'%(taskName))
        
#---------------------------------------------------------------------------------------------------

