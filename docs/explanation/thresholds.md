# How EC2 machine management works - thresholds and idle time

Meadowrun launches EC2 instances when you run jobs. When an instance is no longer running any jobs, Meadowrun will leave it running idle for a configurable amount of time, so that if you run another job soon after, it can reuse the EC2 instance. This reuse is advantageous because starting up an EC2 instance takes time (20 to 40 seconds depending on the instance type, time of day, etc.).

In addition, you can optionally configure thresholds that tell Meadowrun to always makes sure a configurable set of EC2 instances is running (regardless of whether they are idle or not) and effectively exempt these instances from the idle timeout.

Meadowrun allows you to configure one or more thresholds, as well as a maximum time a machine can be idle before it gets shut down. This page explains in more detail how that works.

## Mechanism

In short, Meadowrun triggers a lambda every minute which terminates and starts EC2 instances according to threshold configuration. The lambda is called `meadowrun_ec2_alloc_lambda`.

Two configuration settings affect which machines get terminated, and when:

1. thresholds specify how many resources need to be available;
2. idle time specifies how long to wait before terminating an idle instance, if it's not needed for any threshold.

The lambda executes the following stages:

1. Gathers information about currently running and registered Meadowrun instances.
2. If there are any instances running, but not registered, or any instances registered but not running, then the state of registered instances (as recorded in a DynamoDB table) and running instances (as retrieved via AWS APIs) is reconciled.
3. Terminate any superfluous instances. An instance is superfluous if it's not contributing to any threshold i.e. there are enough other instances running to meet the threshold, and it's been idle for the configured idle timeout.
4. If any thresholds are not met, new instances are started until all thresholds are met.

At every stage, meadowrun tries to minimize cost and "fit" machines into thresholds well. However, since meadowrun prefers to keep instances running over starting new ones, it is possible that there are more resources kept available than strictly necessary to meet thresholds.

## Threshold assignment

Threshold assignment is a sub-stage of stage 3, that determines which of the currently running instances are contributing to thresholds. Those instances should not be terminated.

In a first pass, Meadowrun assigns running instances to thresholds, in order of the thresholds as given in the configuration i.e. the first threshold is assigned instances first. Instances are assigned in order of increasing cost. As a special rule, Meadowrun  avoids assigning expensive instances with GPUs to thresholds that don't have GPU requirements.

In a second pass, any small instances are removed from each assignment, if after removing them the threshold is still met. This way, we avoid assigning many small machines to a threshold, and then a final big machine that makes the contributions of the smaller machines irrelevant. This also favors a few bigger machines over many smaller instances, which makes management easier and is also generally equivalent or better in terms of cost.
