* scheduling policy:
========================
Every time the server receives a request from a client.
The request will be split into several fixed size blocks of i.e. 1000
and then put these blocks into a job queue. When a miner becomes available
or a new miner join in, the miner will go to job queue to check whether
there is a job waiting for executing. If so, the miner will take the first
job out from the queue and execute it. So, we can ensure all the miners
always bear the same workload.

The reason for choosing 1000 as the fixed size is because a typical Andrew
Linux machine can compute SHA-256 hashes at a rate of around 10,000 per
second. Thus, it is reasonable for a miner to do 1000 hashes compute at a time.
