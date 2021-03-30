# Executor

### Local

All jobs run in local event loop, mainly used for debugging


### Process

Jobs are run in different processes.


### Ray

The same as `Process` executor, besides run in a single machines, it's possible to run in different
machines or even cloud services by carefully configuring `Ray`. Check the offical documentation for details.