Installing Meadowrun
====================

.. note::
   Meadowrun requires python 3.7 or above

Install
-------

First, install meadowrun with conda:

.. code-block:: shell

   conda install -c defaults -c conda-forge -c hrichardlee meadowrun


Configure AWS
-------------

Second, make sure you've `configured the AWS CLI
<https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html>`_.


Install meadowrun in your AWS account
-------------------------------------

Finally, set up resources in AWS that e.g. turn off EC2 instances when they're not being
used:

.. code-block:: shell

   meadowrun-manage install

Skipping this step won't prevent you from using meadowrun, but it does mean that once
you start using meadowrun your EC2 instances will run forever.

If you decide to stop using meadowrun, you can remove all meadowrun-related resources
easily:

.. code-block:: shell

   meadowrun-manage uninstall
