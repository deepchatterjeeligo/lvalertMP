description = "a module that holds the interactive queue for lvalert_listenMP"
author = "reed.essick@ligo.org"

#---------------------------------------------------------------------------------------------------

import time
import json
import ConfigParser

import lvalertMPutils as utils

#---------------------------------------------------------------------------------------------------

def interactiveQueue(connection, config_filename, verbose=True, sleep=0.1, maxComplete=100, maxFrac=0.5):
    """
    a simple function that manages a queue

    connection : multiprocessing.connection instance that connects back to lvalert_listenMP
    configname : the path to a config file for this interactiveQueue
    verbose    : whether to print information
    sleep      : the minimum amount of time each epoch will take. If we execute all steps faster than this, we sleep for the remaining time. Keeps us from polling connection too quickly
    timeout    : the amount of time we stay awake after the queue is empty
    buffer     : the extra amount of time we stay alive after timeout to make sure we kill all expected race conditions with lvalert_listenMP
    maxComplete: the maximum number of complete items allowed in the queue before triggering a full traversal to clean them up
    """
    ### determine what type of process this is
    config = ConfigParser.SafeConfigParser()
    config.read( config_filename )

    process_type = config.get('general', 'process_type')
    if verbose:
        print "initializing process_type : %s"%process_type

    if process_type=="test":
        from lvalertMPutils import parseAlert

    elif process_type=="event_supervisor":
        from event_supervisor_utils import parseAlert

    elif process_type=="approval_processorMP":
        from approval_processorMPutils import parseAlert

    else:
        raise ValueError("process_type=%s not understood"%process_type)

    ### set up queue
    queue = utils.SortedQueue() ### instantiate the queue
    queueByGraceID = {} ### hold shorter SortedQueue's, one for each GraceID

    ### iterate
    while True:
        start = time.time()

        ### look for new data in the connection
        if connection.poll():

            ### this blocks until there is something to recieve, which is why we checked first!
            e, t0 = connection.recv()
            if verbose:
                print "received : %s"%e
            e = json.loads(e)

            ### parse the message and insert the appropriate item into the queue
            parseAlert( queue, queueByGraceID, e, t0, config )

        ### remove any completed tasks from the front of the queue
        while len(queue) and queue[0].complete: ### skip all things that are complete already
            item = queue.pop(0) ### note, we expect this to have been removed from queueByGraceID already
            if verobse:
                print "ALREADY COMPLETE: "+item.description

        ### iterate through queue and check for expired things...
        if len(queue):
            if queue[0].hasExpired():
                item = queue.pop(0)
                if verbose:
                    print "performing : %s"%(item.description)

                item.execute( verbose=verbose ) ### now, actually do somthing with that item
                                                     ### note: gdb is a *required* argument to standardize functionality for follow-up processes
                                                     ####      if it is not needed, we should just pass "None"

                if item.complete: ### item is now complete, so we remove it from the queue
                    ### remove this item from queueByGraceID
                    if hasattr(item, 'graceid'): ### QueueItems are not required to have a graceid attribute, but if they do we should manage queueByGraceID
                        queueByGraceID[item.graceid].pop(0) ### this *must* be the first item in this queue too!
                        if not len(queueByGraceID[item.graceid]): ### nothing left in this queue
                            queueByGraceID.pop(item.graceid) ### remove the key from the dictionary

                else: ### item is not complete, so we re-insert it into the queue
                    queue.insert( item )
                    if hasattr(item, 'graceid'): ### QueueItems are not required to have a graceid attribute, but if they do we should manage queueByGraceID
                        queueByGraceID[item.graceid].insert( queueByGraceID[item.graceid].pop(0) ) ### pop and re-insert

            else:
                pass ### do nothing

        ### clean up any empty lists within queueByGraceID
        for graceid in queueByGraceID.keys():
            if not len(queueByGraceID[graceid]): ### nothing in this lists
               queueByGraceID.pop(graceid) ### remove this key from the dictionary
 
        ### check to see if we have too many complete processes in the queue
        if queue.complete > min(len(queue)*maxFrac, maxComplete):
            queue.clean()
 
        ### sleep if needed
        wait = (start+sleep)-time.time() 
        if wait > 0:
            time.sleep(wait)
