description = "a module that contains a sample parseAlert function and supporting methods"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import lvalertMPutils as utils

import logging

from commands import parseCommand
from ligo.lvalert_heartbeat.lvalertMP_heartbeat import parseHeartbeat 

#-------------------------------------------------

class PrintAlertTask(utils.Task):

    name = 'printAlert'

    def __init__(self, timeout, graceid, alert, logTag='iQ'):
        self.logTag = logTag
        self.graceid = graceid
        self.alert = alert
        super(PrintAlertTask, self).__init__(timeout)

    def printAlert(self, verbose=False):
        """
        an example action that we trigger off of an alert
        """
        ### set up logger
        logger = logging.getLogger(self.logTag+'.printAlert') ### verbose means this shows up in iQ's log file
        logger.info( "%s : %s" % (self.graceid, self.alert) )

#-------------------------------------------------

def parseAlert( queue, queueByGraceID, alert, t0, config, logTag='iQ' ):
    """
    figures out what type of action needs to be taken and modifies SortedQueue as needed
    """
    graceid = alert['uid']

    if graceid == 'command': ### this is a command!
        return parseCommand( queue, queueByGraceID, alert, t0, logTag=logTag ) ### delegate and return

    elif graceid == 'heartbeat': ### this is a heartbeat!
        return parseHeartbeat( queue, queueByGraceID, alert, t0, config, logTag=logTag )

    ### set up logger
    logger = logging.getLogger(logTag+'.parseAlert') ### want this to propagate to interactiveQueue's logger

    ### generate the tasks needed
    ### we print the alert twice to ensure the QueueItem works as expected with multiple Tasks
    taskA = PrintAlertTask(  5.0, graceid, alert, logTag=logTag )
    taskB = PrintAlertTask( 10.0, graceid, alert, logTag=logTag )

    ### generate the Item which houses the tasks
    item = utils.QueueItem( t0, [taskA, taskB] )

    ### add the item to the queue
    queue.insert( item )

    ### add the item to the queue for this specific graceID
    if hasattr(item, 'graceid'): ### item must have this attribute for us to add it to queueByGraceID
        if not queueByGraceID.has_key(graceid):
            queueByGraceID[graceid] = SortedQueue()
        queueByGraceID[graceid].insert( item )

    logger.debug( 'added QueueItem=%s'%item.name ) 

    return 0 ### the number of new completed tasks in queue. 
             ### This is not strictly needed and is not captured and we should modify the attribute of SortedQueue directly
