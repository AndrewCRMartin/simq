simq V1.0
=========

-- (c) 2015 UCL, Dr. Andrew C.R. Martin

simq is a very simple program batch queueing system written to meet
the needs ot a web site that has long running high-memory programs
that may be invoked by the user and therefore only one can run at a
time without slowing the machine to a halt.

It simply places jobs in a queue by placing the command in a file that
lives in the 'queuedir' directory. The queue manager then pulls jobs
off one at a time and runs them.


```
Usage:   simq [-v[v...]] [-p polltime] -run queuedir
         simq [-v[v...]] [-w maxwait] queuedir program [parameters ...]
         simq -l queuedir
         -v   Verbose mode (-vv, -vvv more info)
         -p   Specify the wait in seconds between polling for jobs [10]
         -w   Specify maximum wait time when trying to submit a job [60]
         -l   List number of waiting jobs
         -run Run in daemon mode to wait for jobs
```

Note that 'queuedir' must be a full path name.

Queue manager
-------------

The queue manager is invoked by running simq in the backgrounded with
the `-run` flag. e.g.

    nohup nice -10 simq -run /var/tmp/queue1 &

The queue directory ('queuedir') will be created if it does not exist,
with appropriate permissions to allow anybody to write to the
directory. Note there is no restriction on who may submit jobs to the
queue.

Submitting jobs
---------------

To submit a job, run simq as, for example:

    simq /var/tmp/queue1 myprogram param1 param2 

Getting information
-------------------

To obtain the number of waiting jobs, do:

    simq -l /var/tmp/queue1

