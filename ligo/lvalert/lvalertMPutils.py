description = "a module that holds methods and base classes for lvalert_listenMP functionality"
author = "reed.essick@ligo.org"

#---------------------------------------------------------------------------------------------------

import time

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
            raise ValueError("SortedQueue *must* contain only queueItems")

        for item in self.queue: ### iterate through queue and insert where appropriate
            if item.expiration > newItem.expiration:
                self.queue.insert( ind, newItem )
                break
        else:
            self.queue.append( newItem )

    def pop(self, ind):
        return self.queue.pop(ind)

#-------------------------------------------------

class QueueItem(object):
    """
    an item for the sorted Queue
    """

    def __init__(self, graceid, t0, timeout, functionHandle, description="queue Item", *args, **kwargs):
        self.graceid = graceid

        self.t0 = t0
        self.timeout = timeout
        self.expiration = t0 + timeout

        self.functionHandle = functionHandle
        self.args = args

        self.description = description
        self.kwargs = kwargs

        self.complete = False

    def hasExpired(self):
        return not (time.time() < self.expiration)

    def execute(self):
        self.functionHandle( self.graceid, *self.args, **self.kwargs )

#---------------------------------------------------------------------------------------------------

def parseAlert( queue, queueByGraceID, alert, t0, config ):
    """
    figures out what type of action needs to be taken and modifies SortedQueue as needed
    """
    graceid = alert['uid']

    item = QueueItem( graceid, t0, 5.0, printAlert, description="print the alert message", alert=alert )
    queue.insert( item )

    if not queueByGraceID.has_key(graceid):
        queueByGraceID[graceid] = SortedQueue()
    queueByGraceID[graceid].insert( item )

    return 0 ### return the number of *new* complete items that are now in the queue

#-------------------------------------------------

def printAlert( graceid, alert="blah" ):
    """
    an example action that we trigger off of an alert
    """
    print "%s : %s" % (graceid, alert)

