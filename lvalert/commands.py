description = """a module that houses commands that can be sent or received and interpreted within lvalertMP. Also contains the definitions of QueueItems and Tasks needed to respond to commands. 
NOTE: 
this module relies *heavily* on inheritance and standardized naming convetions. The Command, CommandQueueItem, and CommandTask must all have identical name attributes. Then, all that remains is to define the actual execution of the task (via a method retrieved via getattr(task, task.name)) for each Task. This means that to define a new command, we have to define 3 new classes but really only one new method. 
The only bit that requires some care is when we first instantiate the Commands (from within lvalert_commandMP), we must ensure that they have everything that is needed by the associated CommandTask for exectuion stored under their 'object' attribute. WE SHOULD THINK ABOUT GOOD WAYS TO ENSURE THIS IS THE CASE.
"""
author = "reed.essick@ligo.org"

#-------------------------------------------------

import sys

import lvalertMPutils as utils

from numpy import infty

import json 

import types ### needed to build dictionary to reference commands by name

import logging

#-------------------------------------------------
# Define QueueItems and tasks
#-------------------------------------------------
class CommandQueueItem(utils.QueueItem):
    '''
    A parent QueueItem for Commands. This class handles automatic lookup and Task instantiation.
    Most if not all children will simply overwrite the name and description attributes.
    '''
    name = 'command'
    description = 'parent of all command queue items. Implements automatic generation of associated Tasks, etc'

    def __init__(self, t0, queue, queueByGraceID, logTag='iQ', **kwargs):
        tasks = [ __tid__[self.name](queue, queueByGraceID, logTag="%s.%s"%(logTag, self.name), **kwargs) ] ### look up tasks automatically via name attribute

        if kwargs.has_key('graceid'): ### if attached to a graceid, associate it as such
            self.graceid = kwargs['graceid']

        super(CommandQueueItem, self).__init__(t0, tasks, logTag=logTag)

class CommandTask(utils.Task):
    '''
    A parent Task for commands. This class handles automatic identification of functionhandle using self.name. 
    Most children will simply overwrite name and description attributes and define a new method for their actual execution.
    '''
    name = 'command'
    description = "parent of all command tasks"

    required_kwargs  = []
    forbidden_kwargs = []

    def __init__(self, queue, queueByGraceID, logTag='iQ', **kwargs ):
        self.queue = queue
        self.queueByGraceID = queueByGraceID
        if kwargs.has_key('sleep'): ### if this is supplied, we use it
            timeout = kwargs['sleep'] 
        else:
            timeout = -infty ### default is to do things ASAP
        super(CommandTask, self).__init__(timeout, logTag=logTag, **kwargs) ### lookup function handle automatically using self.name
        self.checkKWargs() ### ensure we've set this up correctly. Should be redundant if we construct through Command.genQueueItems. 
                           ### supported in case we create QueueItems directly.

    def checkKWargs(self):
        '''
        checks to make sure we have all the kwargs we need and none of the ones we forbid
        if there's a problem, we raise a KeyError
        '''
        for kwarg in self.required_kwargs: ### check to make sure we have everyting we need. looks up lists within corresponding Command object
            if not self.kwargs.has_key(kwarg):
                raise KeyError('CommandTask=%s is missing required kwarg=%s'%(self.name, kwarg))

        for kwarg in self.forbidden_kwargs: ### check to make sure we don't have anything forbidden. looks up list within corresopnding Command object
            if self.kwargs.has_key(kwarg):
                raise KeyError('CommandTask=%s contains forbidden kwarg=%s'%(self.name, kwarg))

    def command(self, verbose=False, **kwargs):
        '''
        required for syntactic completion
        '''
        pass

#------------------------

class RaiseExceptionItem(CommandQueueItem):
    '''
    QueueItem that raises an exception (ie: kills the process, which should make interactiveQueue fall over)
    '''
    name = 'raiseException'
    description = 'raises a custom made exception'

class RaiseExceptionTask(CommandTask):
    '''
    Task that raises an exception( ie: kills the process, which should make interactiveQUeue fall over)
    '''
    name = 'raiseException'
    description = 'raises a custom made exception'

    required_kwargs  = []
    forbidden_kwargs = []

    def raiseException(self, verbose=False, **kwargs):
        '''
        raises a RuntimeError
        '''
        raise RuntimeError('raiseExeption command received')

#------------------------

class RaiseWarningItem(CommandQueueItem):
    '''
    QueueItem that raises a warning
    '''
    name = 'raiseWarning'
    description = 'raises a custom made warning'

class RaiseWarningTask(CommandTask):
    '''
    Task that raises a warning
    '''
    name = 'raiseWarning'
    description = 'raises a custom made warning'

    required_kwargs  = []
    forbidden_kwargs = []

    def raiseWarning(self, verbose=False, **kwargs):
        '''
        raises a RuntimeWarning
        '''
        raise RuntimeWarning('raiseWarning command received')

#------------------------

class ClearQueueItem(CommandQueueItem):
    '''
    QueueItem that empties the queue
    '''
    name = 'clearQueue'
    description = 'clears the queue'

class ClearQueueTask(CommandTask):
    '''
    Task that empties the queue
    '''
    name = 'clearQueue'
    description = 'clears the queue'

    required_kwargs  = []
    forbidden_kwargs = []

    def clearQueue(self, verbose=False, **kwargs):
        '''
        empties all QueueItems from the queue and from queueByGraceID
        '''
        while len(self.queue): ### empty queue
            self.queue.pop()

        for graceid in self.queueByGraceID.keys(): ### empty queueByGraceID
            self.queueByGraceID.pop(graceid)
        ### note, we don't need to add this back into the queueByGraceID because this Item doesn't have a graceid attribute

#------------------------

class ClearGraceIDQueueItem(CommandQueueItem):
    '''
    QueueItem that empties queue of QueueItems associated with graceid
    '''
    name = "clearGraceID"
    description = "clears queue of items corresponding to 'graceid'"

class ClearGraceIDTask(CommandTask):
    '''
    Task that empties the queue of QueueItems associated with graceid
    '''
    name = "clearGraceID"
    description = "clears queue of items corresponding to 'graceid'"

    required_kwargs  = ['graceid']
    forbidden_kwargs = []

    def clearGraceID(self, verbose=False, **kwargs):
        '''
        empties all QueueItems associated with graceid (required kwarg) from queueByGraceID. 
        Marks these as complete so they are ignored within queue.
        '''
        graceid = self.kwargs['graceid'] ### must be a key of queueByGraceID because this QueueItem has that as an attribute
        ### do NOT remove key, but only iterate over all the other items
        ### the first item (0) MUST be a pointer to this Task's QueueItem, which we leave in
        ### it will be handled within interactiveQueue
        queue = self.queueByGraceID[graceid]
        while len(queue) > 1:
            item = queue.pop(1) ### take the next-to-leading item
            item.complete = True ### mark as complete
            self.queue.complete += 1 ### increment counter within global queue

#------------------------

class CheckpointQueueItem(CommandQueueItem):
    '''
    QueueItem that saves a representation of the queue to disk
    '''
    name = 'checkpointQueue'
    description = 'writes a representation of the queue to disk'

class CheckpointQueueTask(CommandTask):
    '''
    Task that saves a representation of the queue to disk
    '''
    name = 'checkpointQueue'
    description = 'writes a representation of the queue to disk'

    required_kwargs  = ['filename']
    forbidden_kwargs = []

    def checkpointQueue(self, verbose=False, **kwargs):
        '''
        writes a representation of queue into 'filename' (required kwarg)

        WARNING: we may want to gzip or somehow compress the pickle files produced. We'd need to mirror this within loadQueue.
        '''
        import pickle
        filename = self.kwargs['filename']
        file_obj = open(filename, 'w')
        pickle.dump( self.queue, file_obj ) 
        pickle.dump( self.queueByGraceID, file_obj )
        file_obj.close()

#------------------------

class RepeatedCheckpointQueueItem(CommandQueueItem):
    '''
    QueueItem that repeatedly saves a representation of the queue to disk

    note: this is almost identical to CheckpointOueue but we implement separate classes for clarity
    '''
    name = 'repeatedCheckpoint'
    description = 'repeatedly save a representation of the queue to disk'

class RepeatedCheckpointTask(CommandTask):
    '''
    Task that saves a representation of the queue to disk and updates it's own expiration

    note: this is almost identical to CheckpointOueue but we implement separate classes for clarity
    '''
    name = 'repeatedCheckpoint'
    description = 'writes a representation of the queue to disk and updates expiration'

    required_kwargs  = ['filename', 'sleep']
    forbidden_kwargs = []

    def repeatedCheckpoint(self, verbose=False, **kwargs):
        '''
        writes a representation of queue into 'filename' (required kwarg)
        also updates expiration

        WARNING: we may want to gzip or somehow compress the pickle files produced. We'd need to mirror this within loadQueue.
        '''
        import pickle
        filename = self.kwargs['filename']
        file_obj = open(filename, 'w')
        pickle.dump( self.queue, file_obj )
        pickle.dump( self.queueByGraceID, file_obj )
        file_obj.close()

        self.setExpiration(self.expiration) ### update expiration -> self.expiration+self.sleep
                                            ### this is the only substantive difference between RepeatedChecpoint and CheckpointQueue

#------------------------

class LoadQueueItem(CommandQueueItem):
    '''
    QueueItem that loads a representation of the queue from disk
    '''
    name = 'loadQueue'
    description = 'loads a representation of the queue from disk'

class LoadQueueTask(CommandTask):
    '''
    Task that loads a representation of the queue from disk
    '''
    name = 'loadQueue'
    description = 'loads a representation of the queue from disk'

    required_kwargs  = ['filename']
    forbidden_kwargs = []

    def loadQueue(self, verbose=False, **kwargs):
        '''
        loads a representation of queue from 'filename' (required kwarg)

        WARNING: currently, this does not empty the queue first and only adds items from filename into existing SortedQueues
        '''
        ### load queue from pickle file
        import pickle
        filename = self.kwargs['filename']
        file_obj = open(filename, 'r')
        queue = pickle.load(file_obj)
        queueByGraceID = pickle.load(file_obj)
        file_obj.close()

        ### iterate through queue and add it into self.queue and self.queueByGraceID
        for item in queue:
            self.queue.insert( item )

        for graceid, queue in queueByGraceID.items():
            if self.queueByGraceID.has_key(graceid): ### SortedQueue already exists, so insert into it
                for item in queue:
                    self.queueByGraceID[graceid].insert( item )

            else: ### no SortedQueue exists, so just use this one
                self.queueByGraceID[graceid] = queue

#------------------------

class PrintMessageItem(CommandQueueItem):
    '''
    QueueItem that prints a message
    '''
    name = 'printMessage'
    description = 'prints a message'

class PrintMessageTask(CommandTask):
    '''
    Task that prints a message
    '''
    name = 'printMessage'
    description = 'prints a message'

    required_kwargs  = ['message']
    forbidden_kwargs = []

    def printMessage(self, verbose=False, **kwargs):
        '''
        prints 'message' (required kwarg) via a logger
        '''
        ### print set up logger
        logger  = logging.getLogger('%s.%s'%(self.logTag, self.name)) ### want this to also propagate to interactiveQueue's logger
        handler = logging.StreamHandler() ### we don't format this so that it prints exactly as supplied
                                          ### however, interactiveQueue's handler *will* be formatted nicely 
        logger.addHandler( handler )

        ### print to logger
        logger.info( self.kwargs['message'] )

#------------------------

class SendEmailItem(CommandQueueItem):
    '''
    QueueItemm that sends an email
    '''
    name = 'sendEmail'
    description = 'sends an email'

class SendEmailTask(CommandTask):
    '''
    Task that sends an email
    '''
    name = 'sendEmail'
    description = 'sends an email'

    required_kwargs  = ['recipients', 'subject', 'body']
    forbidden_kwargs = []

    def sendEmail(self, verbose=False, **kwargs):
        '''
        sends email via delegation to lvalertMPutils.sendEmail
        '''
        if verbose:
            logger = logging.getLogger('%s.%s'%(self.logTag,self.name)) ### want this to redirect to interactiveQueue's logger
            logger.info( 'sending email to %s'%self.kwargs['recipients'] )

        utils.sendEmail( self.kwargs['recipients'].split(), self.kwargs['body'], self.kwargs['subject'] )

#------------------------

class PrintQueueItem(CommandQueueItem):
    '''
    QueueItem that prints queue and queueByGraceID
    '''
    name = 'printQueue'
    description = 'prints queue and queueByGraceID to a file and will overwrite anything that exists in that path'

class PrintQueueTask(CommandTask):
    '''
    Task that prints queue and queueByGraceID
    '''
    name = 'printQueue'
    description = "prints queue and queueByGraceID to a file and will overwrite anything that exists in that path"

    required_kwargs  = ['filename']
    forbidden_kwargs = []

    def printQueue(self, verbose=False, **kwargs ):
        '''
        prints queue and queueByGraceID to a file
        will overwrite anything existing in that path

        NOTE: if filename=="STDOUT", we default to stdout. if it's "STDERR", we use stderr
        '''
        if verbose: ### print set up logger
            logger = logging.getLogger('%s.%s'%(self.logTag,self.name)) ### want this to redirect to interactiveQueue's logger
            logger.info( 'printing Queue to %s'%self.kwargs['filename'] )

        filename = self.kwargs['filename']
        useSTDOUT = filename=='STDOUT'
        useSTDERR = filename=='STDERR'
        if useSTDOUT:
            file_obj = sys.stdout
        elif useSTDERR:
            file_obj = sys.stderr
        else:
            file_obj = open(filename, 'w')

        print >> file_obj, self.queue
        for graceid, q in self.queueByGraceID.items():
            print >> file_obj, "%s : %s"%(graceid, q)

        if not (useSTDOUT or useSTDERR):
            file_obj.close()

#-------------------------------------------------
# define representations of commands
#-------------------------------------------------

class Command(object):
    '''
    an object based representation of Commands. 
    Each specific command should inherit from from this and provide the following functionality
    '''
    name = 'command'

    def __init__(self, **kwargs):
        self.data = { 'uid'        : 'command',
                      'alert_type' : self.name,
                      'object'     : kwargs,
                    }
        self.checkObject()

    def checkObject(self):
        '''
        ensures that all of the required kwargs are present in self.data['object']
        if something is missing, we raise a KeyError
        also checks to make sure that no forbidden_kwarg is present.
        if one is, we raise a KeyError
        '''
        if not __tid__.has_key(self.name):
            raise KeyError, 'Command=%s is not known!'%self.name

        kwargs = self.data['object']
        for kwarg in __tid__[self.name].required_kwargs: ### check to make sure we have everyting we need
            if not kwargs.has_key(kwarg):
                raise KeyError('Command=%s is missing required kwarg=%s'%(self.name, kwarg))

        for kwarg in __tid__[self.name].forbidden_kwargs: ### check to make sure we don't have anything forbidden
            if kwargs.has_key(kwarg):
                raise KeyError('Command=%s contains forbidden kwarg=%s'%(self.name, kwarg))

    def parse(self, alert):
        '''
        parse a json dictionary from an alert and store data locally
        '''
        if alert['alert_type']==self.name:
            current = self.data
            self.data = alert
            try:
                self.checkObject() ### ensure we have all the kwargs we need

            except KeyError as e:
                self.data = current # restore the previous data so we don't lose it
                raise KeyError, 'failed to parse %s into Command=%s'%(json.dumps(alert), self.name)

        else:
            raise ValueError('cannot parse an command with alert_type=%s within command=%s'%(alert['alert_type'], self.name))

    def write(self):
        '''
        write a json string that can be sent as an alert
        '''
        self.checkObject() ### ensure we have all the kwargs we need
        return json.dumps(self.data)

    def genQueueItems(self, queue, queueByGraceID, t0, logTag='iQ'):
        '''
        defines a list of QueueItems that need to be added to the queue
        uses automatic lookup via __qid__ to identify which QueueItem must be generated based on self.name
        '''
        self.checkObject() ### ensure we have all the kwargs we need
        return [ __qid__[self.name](t0, queue, queueByGraceID, logTag=logTag, **self.data['object']) ] ### look up the QueueItem via qid and self.name, then call the __init__ as needed

#------------------------

class RaiseException(Command):
    '''
    raise an Exception
    '''
    name = 'raiseException'

#------------------------

class RaiseWarning(Command):
    '''
    raise a Warning
    '''
    name = 'raiseWarning'

#------------------------

class ClearQueue(Command):
    '''
    empty the queue
    '''
    name = 'clearQueue'

#------------------------

class ClearGraceID(Command):
    '''
    empties the queue of all items associated with this GraceID
    '''
    name = 'clearGraceID'

#------------------------

class CheckpointQueue(Command):
    '''
    save a representation of the queue to disk

    NOTE: this QueueItem will *not* be included in the pkl file because it will be popped when executed
    '''
    name = 'checkpointQueue'

#------------------------

class RepeatedCheckpoint(Command):
    '''
    save a representation of the queue to disk repeatedly

    NOTE: this QueueItem will *not* be included in the pkl file because it will be popped when executed
    '''
    name = "repeatedCheckpoint"

#------------------------

class LoadQueue(Command):
    '''
    load a representation fo the queue from disk
    '''
    name = 'loadQueue'

#------------------------

class PrintMessage(Command):
    '''
    print a message
    '''
    name = 'printMessage'

#------------------------

class SendEmail(Command):
    '''
    sends an email
    '''
    name = 'sendEmail'

#------------------------

class PrintQueue(Command):
    '''
    print queue and queueByGraceID
    '''
    name = 'printQueue'

#-------------------------------------------------
# define useful variables
#-------------------------------------------------

### set up dictionaries
__cid__ = {} ### Commands by their name attributes
__qid__ = {} ### QueueItems by their name attributes
__tid__ = {} ### Tasks by their name attributes
for x in vars().values():

    if isinstance(x, type):
        if issubclass(x, Command):
            __cid__[x.name] = x
        elif issubclass(x, CommandQueueItem):
            __qid__[x.name] = x
        elif issubclass(x, CommandTask):
            __tid__[x.name] = x

__cid__.pop('command') ### get rid of parent class because we shouldn't be calling it. It's really just a template...
__qid__.pop('command') ### get rid of parent class
__tid__.pop('command') ### get rid of parent class

### confirm that __cid__, __qid__, and __tid__ all have matching keys
assert (sorted(__cid__.keys()) == sorted(__qid__.keys())) and (sorted(__cid__.keys()) == sorted(__tid__.keys())), \
    "inconsistent name attributes within sets of defined Commands, CommandQueueItems, and CommandTasks"

#------------------------
# utilities for looking up info within private variables
#------------------------

def initCommand( name, **kwargs ):
    '''
    wrapper that instantiates Command objects
    '''
    if not __cid__.has_key(name):
        raise KeyError('Command=%s is not known'%name)
    return __cid__[name]( **kwargs )

#-----------

def knownCommands():
    '''
    returns a sorted list of known commands
    '''
    return sorted(__cid__.keys())

#-----------

def requiredKWargs( name ):
    '''
    returns the required KWargs for this command
    '''
    if not __tid__.has_key(name):
        raise KeyError('Command=%s is not known'%name)
    return __tid__[name].required_kwargs

#-----------

def forbiddenKWargs( name ):
    '''
    returns the forbidden KWargs for this command
    '''
    if not __tid__.has_key(name):
        raise KeyError('Command=%s is not known'%name)
    return __tid__[name].forbidden_kwargs

#-------------------------------------------------
# parseCommand
#-------------------------------------------------

def parseCommand( queue, queueByGraceID, alert, t0, logTag='iQ' ):
    '''
    a doppelganger for parseAlert that focuses on commands.
    this should be called from within parseAlert as needed
    '''
    if alert['uid'] != 'command':
        raise ValueError('I only know how to parse alerts with uid="command"')

    ### set up logger
    logger = logging.getLogger('%s.parseCommand'%logTag) ### want this to propagate to interactiveQueue's logger

    cmd = initCommand( alert['alert_type'], **alert['object'] ) ### instantiate the Command object
#    cmd.parse( alert ) ### parse the alert message

    for item in cmd.genQueueItems(queue, queueByGraceID, t0, logTag=logTag): ### add items to the queue
        queue.insert( item )
        if hasattr(item, 'graceid'):
            if not queueByGraceID.has_key(item.graceid):
                queueByGraceID[item.graceid] = utils.SortedQueue()
            queueByGraceID[item.graceid].insert( item )
        logger.debug( 'added Command=%s'%item.name )

    return 0 ### the number of new completed tasks in queue. 
             ### This is not strictly needed and is not captured and we should modify the attribute of SortedQueue directly
