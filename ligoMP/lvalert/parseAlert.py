description = "a module that contains a sample parseAlert function and supporting methods"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import lvalertMPutils as utils

import commands

#-------------------------------------------------

def printAlert( graceid, alert="blah", verbose=False ):
    """
    an example action that we trigger off of an alert
    """
    if verbose:
        print "  print alert"
    print "    %s : %s" % (graceid, alert)

#-------------------------------------------------

def parseAlert( queue, queueByGraceID, alert, t0, config ):
    """
    figures out what type of action needs to be taken and modifies SortedQueue as needed
    """
    graceid = alert['uid']

    if graceid == 'command': ### this is a command!
        return commands.parseCommand( queue, queueByGraceID, alert, t0, config ) ### delegate and return
       
    ### generate the tasks needed
    ### we print the alert twice to ensure the QueueItem works as expected with multiple Tasks
    taskA = utils.Task(  5.0, printAlert, graceid, alert=alert )
    taskB = utils.Task( 10.0, printAlert, graceid, alert=alert )

    ### generate the Item which houses the tasks
    item = utils.QueueItem( t0, [taskA, taskB] )

    ### add the item to the queue
    queue.insert( item )

    ### add the item to the queue for this specific graceID
    if hasattr(item, 'graceid'): ### item must have this attribute for us to add it to queueByGraceID
        if not queueByGraceID.has_key(graceid):
            queueByGraceID[graceid] = SortedQueue()
        queueByGraceID[graceid].insert( item )

    return 0 ### the number of new completed tasks in queue. 
             ### This is not strictly needed and is not captured and we should modify the attribute of SortedQueue directly
