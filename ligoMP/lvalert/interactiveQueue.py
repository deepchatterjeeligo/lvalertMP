description = "a module that holds the interactive queue for lvalert_listenMP"
author = "reed.essick@ligo.org"

#---------------------------------------------------------------------------------------------------

import time
import json

import socket ### used to determine hostname for email warnings
import getpass ### used to determine username for email warnings

from numpy import infty

import ConfigParser

import lvalertMPutils as utils

#---------------------------------------------------------------------------------------------------

def interactiveQueue(connection, config_filename, verbose=True, sleep=0.1, maxComplete=100, maxFrac=0.5, warnThr=1e3, recipients=[], warnDelay=3600, maxWarn=24):
    """
    a simple function that manages a queue

    connection : multiprocessing.connection instance that connects back to lvalert_listenMP
    config_filename : the path to a config file for this interactiveQueue
    verbose    : whether to print information
    sleep      : the minimum amount of time each epoch will take. If we execute all steps faster than this, we sleep for the remaining time. Keeps us from polling connection too quickly

    maxComplete: the maximum number of complete items allowed in the queue before triggering a full traversal to clean them up
    maxFrac    : the maximum fraction of len(queue) that is allowed to be complete before initiating cleanup

    warnThr    : the maximum length of queue before we start sending warning emails
    recipients : list of email addresses that will receive a message if len(queue) > warningThr
    warnDelay  : the amount of time we wait before sending a repeat warning message
    maxWarn    : the maximum amount of warnings we send before silencing this functionality
    """
    ### determine what type of process this is
    config = ConfigParser.SafeConfigParser()
    config.read( config_filename )

    process_type = config.get('general', 'process_type')
    if verbose:
        print "initializing process_type : %s"%process_type

    if process_type=="test":
        from parseAlert import parseAlert

    elif process_type=="event_supervisor":
        from eventSupervisor.eventSupervisor import parseAlert

    elif process_type=="approval_processorMP":
        from approval_processorMP.approval_processorMPutils import parseAlert

    else:
        raise ValueError("process_type=%s not understood"%process_type)

    ### set up queue
    queue          = utils.SortedQueue() ### instantiate the queue
    queueByGraceID = {} ### hold shorter SortedQueue's, one for each GraceID

    ### set up warnings
    warnCount = 0 ### counter for how many warnings we have sent
                  ### we can tell if we've already sent warnings by checking (warnCount>0)
    warnTime = -infty ### the last time we sent a warning
    hostname = socket.gethostbyaddr(socket.gethostname())[0]
    username = getpass.getuser()

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

        ### check len(queue) and send warnings
        if len(queue) > warnThr: ### queue is too long
            if time.time() > warnTime: ### it's not too soon to send another warning
                if recipients: ### send with emails
                    if warnCount < maxWarn: ### we should still send out a warning
                        warnCount += 1 ### increment counter    

                        ### set up the message
                        subject = "WARNING: queue is too long on %s"%(hostname)
                        body    = """WARNING:
interactiveQueue contains SortedQueue with more than %d elements (len(queue)=%d)
    username : %s
    hostname : %s
    config   : %s
This is warning number : %d
"""%(warnThr, len(queue), username, hostname, config_filename, warnCount)

                        if warnCount == maxWarn: ### this is our last warning before silencing, augment message
                            subject = "FINAL "+subject
                            body    = body + "This is the final warning!"

                        utils.sendEmail( recipients )

                    else: ### we've already sent the maximum allowed warnings
                        pass

                else:
                    warnCount = 1 ### set this to a positive number so we'll get a recover notice in the log

                if verbose:
                    print "WARNING: len(queue)=%d <= %d=warnThr"%(len(queue), warnThr)

                warnTime = time.time()+warnDelay ### update time when we'll send the next warning

        elif warnCount > 0: ### we've sent warnings
            warnCount = 0  ### reset this counter because we've recovered
            warnTime = -infty ### reset time of last warning to ensure we send one if things go bad again

            if recipients: ### send RECOVERY notice
                subject = "RECOVERY: SortedQueue has shortened on %s"%(hostname)
                body    = """RECOVERY:
interactiveQueue contains SortedQueue with fewer than %d elements (len(queue)=%d
    username : %s
    hostname : %s
    config   : %s
"""%(warnThr, len(queue), username, hostname, config_filename)

                if warnCount == maxWarn: ### we've silence warnings
                    body = body + "Recovery has un-silenced warnings."

                utils.sendEmail( recipients, body, subject ) 

            if verbose: ### print RECOVERY notice
                print "RECOVERY: len(queue)=%d <= %d=warnThr"%(len(queue), warnThr)
 
        ### sleep if needed
        wait = (start+sleep)-time.time() 
        if wait > 0:
            time.sleep(wait)
