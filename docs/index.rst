Welcome to Meadowrun's documentation!
=====================================

Meadowrun automates the tedious details of running your python code on AWS. Meadowrun
will

- choose the optimal set of EC2 on-demand or spot instances and turn them off when
  they're no longer needed
- deploy your code and libraries to the EC2 instances, so you don't have to worry about
  creating packages and building Docker images
- scale from a single function to thousands of parallel tasks

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Tutorials

   install.rst
   run_function.rst
   run_map.rst

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: How to

   private_git_repo.rst


.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Reference

   apis.rst
