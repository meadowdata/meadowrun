How to use a private git repo
=============================

To use a private git repo, you'll need to give Meadowrun the name of an AWS secret that
contains the private SSH key for the repo you want to use.

First, `create an AWS secret
<https://docs.aws.amazon.com/secretsmanager/latest/userguide/tutorials_basic.html#tutorial-basic-step1>`_
called :code:`my_ssh_key` that contains a key-value pair where the key is
:code:`private_key` and the value is the contents of a private SSH key that has
permission to read your private git repo. If you're using the AWS Console, you'll need
to use the "Plaintext" view and use explicit newline characters to create the
appropriate line breaks. The "plaintext" will look something like:

.. code-block::

    {
      "private_key": "-----BEGIN OPENSSH PRIVATE KEY-----\nLINE1\nLINE2\n-----END OPENSSH PRIVATE KEY-----\n"
    }

Next, the EC2 instances that are running your code (i.e. the role that they run as) will
need to access this secret:

.. code-block::

    meadowrun-manage-ec2 grant-permission-to-secret my_ssh_key

Now you can use the following :code:`Deployment` with :code:`run_function` or
:code:`run_map`:

.. code-block:: python

    Deployment.git_repo(
        "https://github.com/my_organization/my_private_repo",
        conda_yml_file="myenv.yml",
        ssh_key_aws_secret="my_ssh_key"
    )

