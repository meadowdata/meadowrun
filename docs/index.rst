Welcome to Meadowrun's documentation!
=====================================

Meadowrun automates the tedious details of running your python code on AWS. Meadowrun
will

- choose the optimal set of EC2 on-demand or spot instances and turn them off when
  they're no longer needed
- deploy your code and libraries to the EC2 instances, so you don't have to worry about
  creating packages and building Docker images
- scale from a single function to thousands of parallel tasks

For more context, see the `project homepage <https://meadowrun.io>`_.

.. toctree::
   :maxdepth: 1
   :caption: Tutorials

   tutorial/install.rst
   tutorial/run_function.rst
   tutorial/run_map.rst

.. toctree::
   :maxdepth: 1
   :caption: How to

   howto/private_git_repo.rst


.. toctree::
   :maxdepth: 2
   :caption: Reference

   reference/apis.rst
   reference/aws_resources.rst


.. toctree::
   :maxdepth: 1
   :caption: Explanation

   explanation/how_it_works.rst
