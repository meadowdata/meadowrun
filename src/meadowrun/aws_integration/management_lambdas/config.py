from meadowrun.aws_integration.management_lambdas.provisioning import Threshold

# Any changes to these variable names must be reflected in aws_install_uninstall

# 5 minutes is the default, can be overridden on the next line
TERMINATE_INSTANCES_IF_IDLE_FOR_SECS = 5 * 60

INSTANCE_THRESHOLDS = [Threshold(1, logical_cpu_count=2, memory_gb=4)]
