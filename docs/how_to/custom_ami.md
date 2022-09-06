# Use a custom AMI (machine image) on AWS

By default, when you use [AllocEC2Instance][meadowrun.AllocEC2Instance], Meadowrun will
launch EC2 instances using a Meadowrun-managed AMI that has everything Meadowrun needs
to function properly. For example, the Meadowrun-managed AMI has a virtualenv with
Meadowrun pre-installed and a cron job set up to clean up processes that exited
unexpectedly.

You can specify your own AMI in [AllocEC2Instance][meadowrun.AllocEC2Instance] with the
`ami_id` parameter. The AMI that you use must have all of the files/folders in the 
Meadowrun-managed AMI in order to work properly with Meadowrun. The best way to ensure
that this is the case is to build your AMI on top of the Meadowrun-managed AMI.

The easiest way to do this is:

1. Launch a job using Meadowrun that sleeps forever:
```python
import asyncio
import time
import meadowrun

asyncio.run(
    meadowrun.run_function(
        lambda: time.sleep(60 * 60 * 24),
        meadowrun.AllocEC2Instance(),
        meadowrun.Resources(1, 4),
    )
)
```
Make sure the instance is running in the right region, as AMIs are specific to a region.

2. [SSH into that instance](../ssh_to_instance), and make any changes to the machine you
   want to make. For example, you might pre-load a Docker image or layers that are
   commonly used to make cold starts faster.

3. Then, create an AMI as usual (either from the command line or the console). Make sure
to terminate the instance and run `meadowrun-manage-ec2 clean` afterwards. Now you'll be
able to use your new AMI id with Meadowrun:
```python
import asyncio
import time
import meadowrun

asyncio.run(
    meadowrun.run_function(
        lambda: time.sleep(60 * 60 * 24),
        meadowrun.AllocEC2Instance(ami_id="ami-060b22d1a6cc2015d"),
        meadowrun.Resources(1, 4),
    )
)
```

4. Each version of Meadowrun has a new version of the Meadowrun AMI. When you upgrade
the Meadowrun library, you'll need to rebuild your custom AMI on top of the new
Meadowrun AMI.


## Automated AMI creation

> ⚠️  Undocumented behavior

If you want more control over the AMI creation process, you can find the Meadowrun AMI
ids in
[meadowrun.aws_integration.ec2_instance_allocation._AMIS["plain"]](https://github.com/meadowdata/meadowrun/blob/main/src/meadowrun/aws_integration/ec2_instance_allocation.py).
Make sure the version that you're looking at corresponds to the version of Meadowrun
that you're currently using.

You can automated creating AMIs based on these images however you like. For reference,
the Meadowrun images are generated with the
[build_ami.py](https://github.com/meadowdata/meadowrun/blob/main/build_scripts/build_ami.py).
