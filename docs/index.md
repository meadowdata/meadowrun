# Welcome to Meadowrun's documentation!

Meadowrun automates the tedious details of running your python code on AWS or Azure.
Meadowrun will

- choose the optimal set of spot or on-demand EC2 instances/Azure VMs and turn them off
  when they're no longer needed
- deploy your code and libraries to the instances/VMs, so you don't have to worry about
  creating packages and building Docker images
- scale from a single function to thousands of parallel tasks

For more context, see the [project homepage](https://meadowrun.io).
