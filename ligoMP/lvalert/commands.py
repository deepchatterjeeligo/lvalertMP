description = """a module that houses commands that can be sent or received and interpreted within lvalertMP. Also contains the definitions of QueueItems and Tasks needed to respond to commands. 
NOTE: 
this module relies *heavily* on inheritance and standardized naming convetions. The Command, CommandQueueItem, and CommandTask must all have identical name attributes. Then, all that remains is to define the actual execution of the task (via a method retrieved via getattr(task, task.name)) for each Task. This means that to define a new command, we have to define 3 new classes but really only one new method. 
The only bit that requires some care is when we first instantiate the Commands (from within lvalert_commandMP), we must ensure that they have everything that is needed by the associated CommandTask for exectuion stored under their 'object' attribute. WE SHOULD THINK ABOUT GOOD WAYS TO ENSURE THIS IS THE CASE.
"""
author = "reed.essick@ligo.org"

#-------------------------------------------------

import lvalertMPutils as utils

from numpy import infty

import json 

import types ### needed to build dictionary to reference commands by name

#-------------------------------------------------
# Define QueueItems and tasks
#-------------------------------------------------
class CommandQueueItem(utils.QueueItem):
    '''
    FILL ME IN
    '''
    name = 'command'
    description = 'parent of all command queue items. Implements automatic generation of associated Tasks, etc'
    def __init__(self, t0, queue, queueByGraceID, **kwargs):
        tasks = [ tid[self.name](queue, queueByGraceID, **kwargs) ] ### look up tasks automatically via name attribute
        super(CommandQueueItem, self).__init__(t0, tasks)

class CommandTask(utils.Task):
    '''
    FILL ME IN
    '''
    name = 'command'
    description = "parent of all command tasks"
    def __init__(self, queue, queueByGraceID, **kwargs ):
        self.queue = queue
        self.queueByGraceID = queueByGraceID
        if kwargs.has_key('timeout'): ### if this is supplied, we use it
            timeout = kwargs.pop('timeout') ### remove it from kwargs to prevent possible confusion
        else:
            timeout = -infty ### default is to do things ASAP
        super(CommandTask, self).__init__(timeout, getattr(self, self.name), **kwargs) ### lookup function handle automatically using self.name

    def command(self, verbose=False, *args, **kwargs):
        pass

#------------------------

class RaiseExceptionItem(CommandQueueItem):
    '''
    FILL ME IN
    '''
    name = 'raiseException'
    description = 'raises a custom made exception'

class RaiseExceptionTask(CommandTask):
    '''
    FILL ME IN
    '''
    name = 'raiseException'
    description = 'raises a custom made exception'

    def raiseException(self, verbose=False, *args, **kwargs):
        raise NotImplementedError

#------------------------

class RaiseWarningItem(CommandQueueItem):
    '''
    FILL ME IN
    '''
    name = 'raiseWarning'
    description = 'raises a custom made warning'

class RaiseWarningTask(CommandTask):
    '''
    FILL ME IN
    '''
    name = 'raiseWarning'
    description = 'raises a custom made warning'

    def raiseWarning(self, verbose=False, *args, **kwargs):
        raise NotImplementedError

#------------------------

class ClearQueueItem(CommandQueueItem):
    '''
    FILL ME IN
    '''
    name = 'clearQueue'
    description = 'clears the queue'

class ClearQueueTask(CommandTask):
    '''
    FILL ME IN
    '''
    name = 'clearQueue'
    description = 'clears the queue'

    def clearQueue(self, verbose=False, *args, **kwargs):
        raise NotImplementedError

#------------------------

class CheckpointQueueItem(CommandQueueItem):
    '''
    FILL ME IN
    '''
    name = 'checkpointQueue'
    description = 'writes a representation of the queue to disk'

class CheckpointQueueTask(CommandTask):
    '''
    FILL ME IN
    '''
    name = 'checkpointQueue'
    description = 'writes a representation of the queue to disk'

    def checkpointQueue(self, verbose=False, *args, **kwargs):
        raise NotImplementedError

#------------------------

class LoadQueueItem(CommandQueueItem):
    '''
    FILL ME IN
    '''
    name = 'loadQueue'
    description = 'loads a representation of the queue from disk'

class LoadQueueTask(CommandTask):
    '''
    FILL ME IN
    '''
    name = 'loadQueue'
    description = 'loads a representation of the queue from disk'

    def loadQueue(self, verbose=False, *args, **kwargs):
        raise NotImplementedError

#------------------------

class PrintMessageItem(CommandQueueItem):
    '''
    FILL ME IN
    '''
    name = 'printMessage'
    description = 'prints a message to stdout'

class PrintMessageTask(CommandTask):
    '''
    FILL ME IN
    '''
    name = 'printMessage'
    description = 'prints a message to stdout'

    def printMessage(self, verbose=False, *args, **kwargs):
        raise NotImplementedError

#-------------------------------------------------
# define representations of commands
#-------------------------------------------------

class Command:
    '''
    an object based representation of Commands. 
    Each specific command should inherit from from this and provide the following functionality
    '''
    name = 'command'

    def __init__(self, command_type='command', **kwargs):
        self.data = { 'uid'        : 'command',
                      'alert_type' : command_type,
                      'object'     : kwargs,
                    }

    def parse(self, alert):
        if alert['alert_type']==self.name:
            self.data = alert
        else:
            raise ValueError('cannot parse an command with alert_type=%s within command=%s'%(alert['alert_type'], self.name))

    def write(self):
        return json.dumps(self.data)

    def genQueueItems(self, queue, queueByGraceID, t0):
        '''
        defines a list of QueueItems that need to be added to the queue
        uses automatic lookup via qid to identify which QueueItem must be generated based on self.name
        '''
        return [ qid[self.name](t0, queue, queueByGraceID, **self.data['object']) ] ### look up the QueueItem via qid and self.name, then call the __init__ as needed

#------------------------

class RaiseException(Command):
    '''
    FILL ME IN
    '''
    name = 'raiseException'
    def __init__(self, **kwargs):
        super(RaiseException, self).__init__(command_type=self.name, **kwargs)

#------------------------

class RaiseWarning(Command):
    '''
    FILL ME IN
    '''
    name = 'raiseWarning'
    def __init__(self, **kwargs):
        super(RaiseWarning, self).__init__(command_type=self.name, **kwargs)

#------------------------

class ClearQueue(Command):
    '''
    FILL ME IN
    '''
    name = 'clearQueue'
    def __init__(self, **kwargs):
        super(ClearQueue, self).__init__(command_type=self.name, **kwargs)

#------------------------

class CheckpointQueue(Command):
    '''
    FILL ME IN
    '''
    name = 'checkpointQueue'
    def __init__(self, **kwargs):
        super(CheckpointQueue, self).__init__(command_type=self.name, **kwargs)

class LoadQueue(Command):
    '''
    FILL ME IN
    '''
    name = 'loadQueue'
    def __init__(self, **kwargs):
        super(LoadQueue, self).__init__(command_type=self.name, **kwargs)

#------------------------

class PrintMessage(Command):
    '''
    FILL ME IN
    '''
    name = 'printMessage'
    def __init__(self, **kwargs):
        super(PrintMessage, self).__init__(command_type=self.name, **kwargs)

#-------------------------------------------------
# define useful variables
#-------------------------------------------------

### set up dictionaries
cid = {} ### Commands by their name attributes
qid = {} ### QueueItems by their name attributes
tid = {} ### Tasks by their name attributes
for x in vars().values():

    if isinstance(x, types.ClassType):
        if issubclass(x, Command):
            cid[x.name] = x

    elif isinstance(x, type):
        if issubclass(x, CommandQueueItem):
            qid[x.name] = x
        elif issubclass(x, CommandTask):
            tid[x.name] = x

cid.pop('command') ### get rid of parent class because we shouldn't be calling it. It's really just a template...
qid.pop('command') ### get rid of parent class
tid.pop('command') ### get rid of parent class

#-------------------------------------------------
# parseCommand
#-------------------------------------------------

def parseCommand( queue, queueByGraceID, alert, t0):
    '''
    a doppelganger for parseAlert that focuses on commands.
    this should be called from within parseAlert as needed
    '''
    if alert['uid'] != 'command':
        raise ValueError('I only know how to parse alerts with uid="command"')

    cmd = cid[alert['alert_type']]() ### instantiate the Command object
    cmd.parse( alert ) ### parse the alert message

    for item in cmd.genQueueItems(queue, queueByGraceID, t0): ### add items to the queue
        queue.insert( item )
        if hasattr(item, 'graceid'):
            queueByGraceID[item.graceid].insert( item )

    return 0 ### the number of new completed tasks in queue. 
             ### This is not strictly needed and is not captured and we should modify the attribute of SortedQueue directly
