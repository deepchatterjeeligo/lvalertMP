# lvalertMP
a place holder for lvalertMP functionality

I've dumped the important lvalertMP code in here to document it in a place that is not my laptop.

Note, this is not a "well formatted" package or anything, but just a place for me too back-up the code as it evolves.
The actual development will be done in a working copy of the lvalert repo, where I hope most of this will eventually reside.

-------------------
User's Guide
-------------------
To use lvalert_listenMP, you must supply a properly formatted config file. There is an example in ~/etc/lvalert_listenMP-example.ini. Note, the structure is different from lvalert_listen in several important ways

  1) There will be a single section for each child process, and multiple lvalert nodes can be assigned to each child. 

  2) users supply a path to a "childConfig" rather than an executable. The childConfig tells the code what to run and is standardized within the "InteractiveQueue" module.

If we look at ~/etc/childConfig-example.ini, we see several things. First, in the [general] section there is an option for "process_type". Currently, we only support "test", "event_supervisor" and "approval_processor". This option tells the child process which libraries to load. It is the basic requirement of the childConfig, although specific applications (such as event_supervisor) will require more information.

To use this library, you must supply a correctly formatted config file but otherwise the user interface should be exactly like lvalert_listen. For example:

    lvalert_listenMP -a userName -b password -r resource -c ./lvalert_listenMP_test.ini

-------------------
Implementation details
-------------------
For an example of how all this hangs together, please take a look at the event_supervisor library written for lvalertMP

    https://github.com/reedessick/event_supervisor2

That repository demonstrates how to extend the classes defined here to implemente specific functionality.

lvalert_listenMP primarily differs from lvalert_listen in that it forks via Python's multiprocessing module instead of the subprocess module. This means that child processes are created (and live in perpetuity) by lvalert_listenMP. As alerts are received, lvalert_listenMP directs the json strings through a pipe to the corresponding child process. The child process receives the message and updates an "interactiveQueue" to react accordingly. Note, if any child process dies, the entire lvalert_listenMP process will raise an exception and terminate.

The "interactiveQueue" is stored in ~/ligo/lvalert/interactiveQueue.py and is currently just a function defined therein. This function is what is actually forked from lvalert_listenMP and handles all the inter-process communications for the user. **USERS SHOULD NOT HAVE TO MODIFY EITHER lvalert_listenMP OR interactiveQueue!** However, it is useful to understand how the code works. The interactiveQueue alternatively checks for new alerts (pass along by lvalert_listenMP) and checks whether it needs to perform actions from the queue. It does this within a simple while loop and manages an instance of a "SortedQueue" mostly via delegation. The "SortedQueue" and associated "QueueItem" and "Task" classes are defined in another module: lvalertMPutils.py. **USERS SHOULD DEFINE EXTENSIONS OF lvalertMPutils.py via inheritence to implement new functionality.** Their library can then be added to the available options around line 31 of interactiveQueue.py.

lvalertMPutils provides the main work-horse of the engine, including parsing alerts and defining queues, queue items, and tasks. The module provided is meant to provide examples and base classes that can be extended or overwritten in user-defined modules as needed. We highly recommend using inheritence as much as possible to allow the default classes to manage many of the necessary tasks. Within lvalertMPutils, we define the following key functions and classes that users will need to re-define or extend

  - parseAlert( queue, queueByGraceID, alert, t0, config, timeout=5.0 )
        this is **required** by interactiveQueue and will be used to determine how the process should react to lvalert messages. Note, parsing alerts may mean that QueueItems or their associated Tasks are no longer needed and should be marked "complete". Because we do not necessarily know their position in the SortedQueue, eath QueueItem and Task has a "complete" attribute. Changing these to True will cause the SortedQueue to automatically clean them up.
        NOTE: there are several instances of a SortedQueue which are managed simulaneously (for look-up efficiency). See below for a more complete discussion.
  - QueueItem
        this is a class which is used within the SortedQueue. QueueItems contain lists of Tasks that need to be completed. In this way, each QueueItem can represent a single "follow-up process" that does several things rather than defining separate items for small actions performed by the follow-up process.
  - Task
        this is the basic "job" that the engine needs to perform. Tasks have an execute() method, which is called as needed and will perform the actual work.

as well as some classes that should **not** be modified or extended.

  - SortedQueue
        this is a basic queue that sorts it's items based on their expiration times. This sorting allows the interactiveQueue to efficientily identify which QueueItems need attention and when.
        Right now, insertion and removal scale roughly linearly with the queue's size. We may be able to improve upon this in the future.


parseAlert and interactiveQueue manage several instances of the SortedQueue class. 
  - The instance called "queue" contains all QueueItems corresponding to all GraceDB events. "queue" is what is used to determine which QueueItem is next when multiple GraceDB events are being tracked at the same time. 
  - queueByGraceID is a dictionary with key=GraceID and value=SortedQueue(). In this way, when a QueueItem comes up in "queue" or a new alert comes in, the code can efficiently identify which group of QueueItems need to be managed by looking up the smaller SortedQueue that only contains Items for this GraceDB entry. We note that all manipulations of queueByGraceID are automatic **if and only if** the QueueItem's expiration is reached and it's execute() method is called. **However**, if parseAlert marks an Item as complete it **must** remove it from the associated SortedQueue stored within queueByGraceID otherwise it will never be removed. InteractiveQueue automatically cleans up the instance called "queue" because it can do so with efficient look-up but does not iterate through queueByGraceID.

-------------------
To Do
-------------------

-- implement checkpointing and auto-recovery
-- implement command line interface to communicate with a running process via lvalert_send, etc
-- change default ``test'' to also include QueueItems with multiple taks so that this functionality is exercised and demonstrated.
