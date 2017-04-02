description = "a module that holds the interactive queue for lvalert_listenMP"
author = "reed.essick@ligo.org"

#---------------------------------------------------------------------------------------------------

import os

import time
import json

import socket ### used to determine hostname for email warnings
import getpass ### used to determine username for email warnings

from numpy import infty

import ConfigParser

import lvalertMPutils as utils

import logging
import traceback

#---------------------------------------------------------------------------------------------------

### set up email warning templates

lvalert_subject = "WARNING: could not parse lvalert payload on %s"
lvalert_body    = """\
time (localtime): 
  %s

lvalert message: 
  %s

%s

    username : %s
    hostname : %s
    config   : %s
"""

parseAlert_subject = "WARNING: parseAlert caught an exception on %s"
parseAlert_body    = """\
time (localtime): 
  %s

lvalert message: 
  %s

%s

    username : %s
    hostname : %s
    config   : %s
"""
execute_subject = "WARNING: %s.execute caught an exception on %s"
execute_body    = """\
time (localtime): 
  %s

QueueItem = %s: 
  %s

%s

    username : %s
    hostname : %s
    config   : %s

QueueItem marked complete to avoid repeated errors.
"""
warning_subject = "WARNING: queue is too long on %s"
warning_body    = """WARNING:
interactiveQueue contains SortedQueue with more than %d elements (len(queue)=%d)
    username : %s
    hostname : %s
    config   : %s
This is warning number : %d
"""
recovery_subject = "RECOVERY: SortedQueue has shortened on %s"
recovery_body    = """RECOVERY:
interactiveQueue contains SortedQueue with fewer than %d elements (len(queue)=%d)
    username : %s
    hostname : %s
    config   : %s
"""
#---------------------------------------------------------------------------------------------------

def interactiveQueue(connection, config_filename, verbose=True, sleep=0.1, maxComplete=100, maxFrac=0.5, warnThr=1e3, recipients=[], warnDelay=3600, maxWarn=24, print2stdout=False):
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
    ### load in config file
    config = ConfigParser.SafeConfigParser()
    config.read( config_filename )

    ### extract high level parameters
    process_type = config.get('general', 'process_type')
    logDir       = config.get('general', 'log_directory') if config.has_option('general', 'log_directory') else "."
    logLevel     = config.getint('general', 'log_level') if config.has_option('general', 'log_level') else 10

    ### set up logger
    ### this logger will capture *everything* that is printed through a child logger
    if verbose:
        logger = logging.getLogger("iQ")
        logger.setLevel(logLevel) ### NOTE: may want to make this an option in config file

        ### set up handlers
        # into a file with a predictable filename
        handlers = [logging.FileHandler(utils.genLogname(logDir, process_type+'_'+os.path.basename(config_filename).strip('.ini')))]
        if print2stdout:
            handlers.append(logging.StreamHandler())

        for handler in handlers:
            handler.setFormatter( utils.genFormatter() )
            logger.addHandler( handler )

        logger.info( "using config : %s"%config_filename )
        logger.info( "initializing process_type : %s"%process_type )

    ### set up libraries depending on process_type
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
                logger.info( "received : %s"%e )

            try:
                e = json.loads(e)

            except Exception:
                trcbk = traceback.format_exc().strip("\n")
                if verbose:
                    logger.warn( 'could not parse lvalert payload!' )
                    logger.warn( trcbk )

                if recipients:
                    utils.sendEmail( 
                        recipients, 
                        lvalert_body%(time.ctime(t0), e, trcbk, username, hostname, config_filename), 
                        lvalert_subject%(hostname),
                    )

            ### parse the message and insert the appropriate item into the queuie
            ### only do this if "e" was successfully parsed into a dictionary
            else:
                try:
                    parseAlert( queue, queueByGraceID, e, t0, config )

                except Exception:
                    trcbk = traceback.format_exc().strip("\n")
                    if verbose:
                        logger.warn( 'parseAlert raised an exception!' )
                        logger.warn( trcbk )

                    if recipients:
                        utils.sendEmail( 
                            recipients, 
                            parseAlert_body%(time.ctime(t0), json.dumps(e), trcbk, username, hostname, config_filename), 
                            parseAlert_subject%(hostname),
                        )

        ### remove any completed tasks from the front of the queue
        while len(queue) and queue[0].complete: ### skip all things that are complete already
            item = queue.pop(0) ### note, we expect this to have been removed from queueByGraceID already
            if verbose:
                logger.debug( "ALREADY COMPLETE: "+item.description )

        ### iterate through queue and check for expired things...
        if len(queue):
            if queue[0].hasExpired():
                item = queue.pop(0)
                if verbose:
                    logger.info( "performing : %s"%(item.description) )

                ### now, actually do something with that item
                try: 
                    item.execute( verbose=verbose ) 

                except Exception:
                    item.complete = True ### mark this as complete so we don't repeatedly hit the same error
                                         ### NOTE: this may cause other formatting errors if this item modified queue or queueByGraceId
                                         ###       and failed before those were complete...

                    trcbk = traceback.format_exc().strip("\n")
                    if verbose:
                        logger.warn( '%s\'s execute raised an exception! Marking QueueItem as complete to avoid repeated errors.'%item.name )
                        logger.warn( trcbk )

                    if recipients:
                        utils.sendEmail( 
                            recipients, 
                            execute_body%(time.ctime(t0), item.name, item.description, trcbk, username, hostname, config_filename),
                            execute_subject%(item.name, hostname),
                        )

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
                warnCount += 1
                if recipients: ### send with emails
                    if warnCount <= maxWarn: ### we should still send out a warning
                        ### set up the message
                        subject = warning_subject%(hostname)
                        body    = warning_body%(warnThr, len(queue), username, hostname, config_filename, warnCount)

                        if warnCount == maxWarn: ### this is our last warning before silencing, augment message
                            subject = "FINAL "+subject
                            body    = body + "This is the final warning!"

                        utils.sendEmail( recipients, body, subject )

                    else: ### we've already sent the maximum allowed warnings
                        pass

                if verbose:
                    logger.warn( "len(queue)=%d >= %d=warnThr; emails sent to : %s"%(len(queue), warnThr, ", ".join(recipients)) )

                warnTime = time.time()+warnDelay ### update time when we'll send the next warning

        elif warnCount > 0: ### we've sent warnings
            if recipients: ### send RECOVERY notice
                body = recovery_body%(warnThr, len(queue), username, hostname, config_filename)

                if warnCount >= maxWarn: ### we've silence warnings
                    body = body + "Recovery has un-silenced warnings."

                utils.sendEmail( recipients, body, recovery_subject%(hostname) ) 

            if verbose: ### print RECOVERY notice
                logger.warn( "len(queue)=%d <= %d=warnThr; emails sent to : %s"%(len(queue), warnThr, ", ".join(recipients)) )
 
            warnCount = 0  ### reset this counter because we've recovered
            warnTime = -infty ### reset time of last warning to ensure we send one if things go bad again

        ### sleep if needed
        wait = (start+sleep)-time.time() 
        if wait > 0:
            time.sleep(wait)
