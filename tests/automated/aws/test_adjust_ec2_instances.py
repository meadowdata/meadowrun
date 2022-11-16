from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Tuple
from meadowrun.aws_integration.ec2_instance_allocation import AllocEC2Instance

# from meadowrun.aws_integration.ec2_instance_allocation import AllocEC2Instance
from meadowrun.aws_integration.management_lambdas.adjust_ec2_instances import (
    _EC2_Instance,
    _add_deregister_and_terminate_actions,
    _sync_registration_and_actual_state,
)
from meadowrun.aws_integration.management_lambdas.provisioning import (
    Threshold,
    shutdown_thresholds,
)
from meadowrun.instance_selection import (
    CloudInstanceType,
    OnDemandOrSpotType,
    ResourcesInternal,
)
from meadowrun.config import (
    AVX,
    EVICTION_RATE_INVERSE,
    GPU,
    GPU_MEMORY,
    LOGICAL_CPU,
    MEMORY_GB,
    EPHEMERAL_STORAGE_GB,
)
from meadowrun.run_job_core import Resources


if TYPE_CHECKING:
    from typing_extensions import Final
    from pytest_mock import MockerFixture
    from mypy_boto3_ec2.service_resource import Instance


MOCK_TYPE_TO_INFO: Dict[Tuple[str, OnDemandOrSpotType], CloudInstanceType] = {
    ("XS", "spot"): CloudInstanceType(
        "XS",
        "spot",
        0.05,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 1, MEMORY_GB: 2},
            non_consumable={EVICTION_RATE_INVERSE: 90},
        ),
    ),
    ("S", "spot"): CloudInstanceType(
        "S",
        "spot",
        0.1,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 2, MEMORY_GB: 4},
            non_consumable={EVICTION_RATE_INVERSE: 90},
        ),
    ),
    ("M", "spot"): CloudInstanceType(
        "M",
        "spot",
        0.2,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 4, MEMORY_GB: 8},
            non_consumable={EVICTION_RATE_INVERSE: 90},
        ),
    ),
    ("XS-GPU", "spot"): CloudInstanceType(
        "XS-GPU",
        "spot",
        0.3,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 1, MEMORY_GB: 2, GPU: 1, GPU_MEMORY: 10},
            non_consumable={EVICTION_RATE_INVERSE: 90},
        ),
    ),
    ("S-GPU", "spot"): CloudInstanceType(
        "S-GPU",
        "spot",
        0.6,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 2, MEMORY_GB: 4, GPU: 2, GPU_MEMORY: 20},
            non_consumable={EVICTION_RATE_INVERSE: 90},
        ),
    ),
    ("M-GPU", "spot"): CloudInstanceType(
        "M-GPU",
        "spot",
        1.2,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 4, MEMORY_GB: 8, GPU: 4, GPU_MEMORY: 40},
            non_consumable={EVICTION_RATE_INVERSE: 90},
        ),
    ),
    ("AVX1", "spot"): CloudInstanceType(
        "XS",
        "spot",
        0.06,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 1, MEMORY_GB: 2},
            non_consumable={AVX: 1.0, EVICTION_RATE_INVERSE: 90},
        ),
    ),
    ("AVX2", "spot"): CloudInstanceType(
        "XS",
        "spot",
        0.07,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 1, MEMORY_GB: 2},
            non_consumable={AVX: 2.0, EVICTION_RATE_INVERSE: 90},
        ),
    ),
    ("AVX3", "spot"): CloudInstanceType(
        "XS",
        "spot",
        0.08,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 1, MEMORY_GB: 2},
            non_consumable={AVX: 3.0, EVICTION_RATE_INVERSE: 90},
        ),
    ),
}


def test_shutdown_thresholds_single_threshold() -> None:
    thresholds = [Threshold(Resources(logical_cpu=2, memory_gb=4))]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {"id1": ("S", "spot")}
    instance_to_resource = {f"id{i}": ResourcesInternal({}, {}) for i in range(1, 4)}

    # S instance matches exactly with threshold
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert terminated_instance_ids == []
    assert unmet_thresholds == []

    # an extra S instance, one of them should shut down
    instances["id2"] = ("S", "spot")
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert len(terminated_instance_ids) == 1
    assert unmet_thresholds == []

    # an extra XS instance, too small for threshold.
    # Should shut down, as well as one of id1 or id2.
    instances["id3"] = ("XS", "spot")
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert len(terminated_instance_ids) == 2
    assert "id3" in terminated_instance_ids
    assert unmet_thresholds == []


def test_shutdown_thresholds_single_threshold_resource_too_small() -> None:
    # XS instance is too small for threshold, should shut down
    thresholds = [Threshold(Resources(logical_cpu=2, memory_gb=4))]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {"id1": ("XS", "spot")}
    instance_to_resource = {f"id{i}": ResourcesInternal({}, {}) for i in range(1, 2)}

    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert terminated_instance_ids == ["id1"]
    assert unmet_thresholds == [(thresholds[0], 1)]


def test_shutdown_thresholds_single_threshold_resource_bigger() -> None:
    # M instance is bigger than threshold, should keep it
    thresholds = [Threshold(Resources(logical_cpu=2, memory_gb=4))]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {"id1": ("M", "spot")}
    instance_to_resource = {f"id{i}": ResourcesInternal({}, {}) for i in range(1, 3)}
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert terminated_instance_ids == []
    assert unmet_thresholds == []

    # id2 is S instance, cheaper to keep this one.
    instances["id2"] = ("S", "spot")
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert terminated_instance_ids == ["id1"]
    assert unmet_thresholds == []


def test_shutdown_thresholds_single_threshold_keep_cheapest_resource() -> None:
    # M instance is bigger than threshold, should keep it
    thresholds = [Threshold(Resources(logical_cpu=1, memory_gb=2))]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        "id1": ("S", "spot"),
        "id2": ("XS", "spot"),
        "id3": ("M", "spot"),
        "id4": ("S", "spot"),
    }
    instance_to_resource = {f"id{i}": ResourcesInternal({}, {}) for i in range(1, 5)}
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert set(terminated_instance_ids) == set(["id1", "id3", "id4"])
    assert unmet_thresholds == []


def test_shutdown_thresholds_single_threshold_resource_has_gpu() -> None:
    # S-GPU has the right size for threshold, but has GPU. Should shut down.
    thresholds = [Threshold(Resources(logical_cpu=2, memory_gb=4))]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {"id1": ("S-GPU", "spot")}
    instance_to_resource = {f"id{i}": ResourcesInternal({}, {}) for i in range(1, 3)}
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert terminated_instance_ids == ["id1"]
    assert unmet_thresholds == [(thresholds[0], 1)]

    # S instance has right size. Should not shut down.
    instances["id2"] = ("S", "spot")
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert terminated_instance_ids == ["id1"]
    assert unmet_thresholds == []


def test_shutdown_thresholds_single_threshold_multiple_times() -> None:
    thresholds = [Threshold(Resources(logical_cpu=2, memory_gb=4), num_resources=4)]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        f"id{i}": ("S", "spot") for i in range(4)
    }
    instance_to_resource = {f"id{i}": ResourcesInternal({}, {}) for i in range(7)}

    # S instance matches exactly with threshold
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert terminated_instance_ids == []
    assert unmet_thresholds == []

    # an extra S instance, one of them should shut down
    instances["id4"] = ("S", "spot")
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert len(terminated_instance_ids) == 1
    assert unmet_thresholds == []

    # an extra M instance, one of them should shut down
    instances["id5"] = ("M", "spot")
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert len(terminated_instance_ids) == 2
    assert "id5" in terminated_instance_ids
    assert unmet_thresholds == []

    # an extra XS instance, too small for threshold.
    # Should shut down, as well as one of id1 or id2.
    instances["id6"] = ("XS", "spot")
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert len(terminated_instance_ids) == 3
    assert "id5" in terminated_instance_ids
    assert "id6" in terminated_instance_ids
    assert unmet_thresholds == []


def test_shutdown_thresholds_multiple_thresholds() -> None:
    thresholds = [
        Threshold(Resources(logical_cpu=2, memory_gb=4)),
        Threshold(Resources(gpus=1, gpu_memory=10)),
    ]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        "id0": ("S", "spot"),
        "id1": ("XS-GPU", "spot"),
    }
    instance_to_resource = {f"id{i}": ResourcesInternal({}, {}) for i in range(5)}

    # S/XS-GPU instance matches exactly with one threshold each
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert terminated_instance_ids == []
    assert unmet_thresholds == []

    # an extra S instance, one of them should shut down
    instances["id2"] = ("S", "spot")
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert len(terminated_instance_ids) == 1
    assert "id1" not in terminated_instance_ids
    assert unmet_thresholds == []

    # an extra S-GPU instance, one of them should shut down
    instances["id3"] = ("S-GPU", "spot")
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert len(terminated_instance_ids) == 2
    assert "id3" in terminated_instance_ids
    assert unmet_thresholds == []

    # an extra XS instance, too small for threshold.
    # Should shut down, as well as one of id1 or id2.
    instances["id4"] = ("XS", "spot")
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert len(terminated_instance_ids) == 3
    assert "id3" in terminated_instance_ids
    assert "id4" in terminated_instance_ids
    assert unmet_thresholds == []


def test_shutdown_thresholds_multiple_thresholds_multiple_times() -> None:
    thresholds = [
        Threshold(Resources(logical_cpu=2, memory_gb=4), num_resources=5),
        Threshold(Resources(gpus=1, gpu_memory=10), num_resources=2),
    ]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        f"id{i}": ("S", "spot") for i in range(4)
    }
    instance_to_resource = {f"id{i}": ResourcesInternal({}, {}) for i in range(6)}
    instances.update({f"gpu-id{i}": ("XS-GPU", "spot") for i in range(1)})
    instance_to_resource.update(
        {f"gpu-id{i}": ResourcesInternal({}, {}) for i in range(2)}
    )

    # not enough resources to match threshold
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert terminated_instance_ids == []
    assert unmet_thresholds == [(thresholds[0], 1), (thresholds[1], 1)]

    # an extra S instance, should be shut down as it's too small
    instances["id4"] = ("XS", "spot")
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert terminated_instance_ids == ["id4"]
    assert unmet_thresholds == [(thresholds[0], 1), (thresholds[1], 1)]

    # an extra S instance, matches first threshold exactly
    instances["id5"] = ("S", "spot")
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert terminated_instance_ids == ["id4"]
    assert unmet_thresholds == [(thresholds[1], 1)]

    # an extra GPU instance, should shut down the smaller GPU machine
    instances["gpu-id1"] = ("M-GPU", "spot")
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert set(terminated_instance_ids) == {"id4", "gpu-id0"}
    assert unmet_thresholds == []


def test_shutdown_thresholds_second_pass() -> None:
    thresholds = [
        Threshold(Resources(logical_cpu=2, memory_gb=4), num_resources=5),
    ]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        f"id{i}": ("S", "spot") for i in range(4)
    }
    instance_to_resource = {f"id{i}": ResourcesInternal({}, {}) for i in range(5)}
    instances["big"] = ("M", "spot")
    instance_to_resource["big"] = ResourcesInternal({}, {})

    # should keep the M instance, and just enough of the smaller ones to reach threshold
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert len(terminated_instance_ids) == 1
    assert "big" not in terminated_instance_ids
    assert unmet_thresholds == []

    # this doesn't work, because big2 is not even assigned in the first pass, so the
    # second pass can't remove it.
    # instances["big2"] = ("M", "spot")

    # # can shut down two more of the S instances, keep the two M.
    # terminated_instance_ids = shutdown_thresholds(
    # thresholds, instances,MOCK_TYPE_TO_INFO)
    # assert len(terminated_instance_ids) == 3
    # assert "big" not in terminated_instance_ids
    # assert "big2" not in terminated_instance_ids


def test_shutdown_thresholds_flags() -> None:
    thresholds = [
        Threshold(Resources(logical_cpu=1, memory_gb=2, flags="AVX2"), num_resources=2),
    ]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        "id0": ("AVX1", "spot"),
        "id1": ("AVX2", "spot"),
        "id2": ("AVX3", "spot"),
        "id3": ("AVX1", "spot"),
    }
    instance_to_resource = {f"id{i}": ResourcesInternal({}, {}) for i in range(0, 4)}

    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, MOCK_TYPE_TO_INFO, instance_to_resource
    )
    assert set(terminated_instance_ids) == {"id0", "id3"}
    assert unmet_thresholds == []


def _get_ec2_type_info_snapshot() -> Dict[
    Tuple[str, OnDemandOrSpotType], CloudInstanceType
]:
    with open(
        Path(os.path.dirname(__file__)) / "snapshot-aws_ec2_prices-us-east-2.json",
        "r",
        encoding="utf-8",
    ) as f:
        json_file = json.load(f)
        cloud_instance_types = [CloudInstanceType.from_dict(cit) for cit in json_file]
    return {
        (instance_type.name, instance_type.on_demand_or_spot): instance_type
        for instance_type in cloud_instance_types
    }


EC2_TYPE_TO_INFO: Final = _get_ec2_type_info_snapshot()


def test_ec2_have_at_least_20_1CPU_4GB_available() -> None:
    thresholds = [
        Threshold(Resources(logical_cpu=1, memory_gb=4), num_resources=20),
    ]

    # 2cpu 16Gb
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        f"lar{i}": ("r6a.large", "spot") for i in range(5)
    }
    instance_to_resource = {f"lar{i}": ResourcesInternal({}, {}) for i in range(5)}
    # 2cpu 4Gb
    instances.update({f"med{i}": ("t3.medium", "spot") for i in range(10)})
    instance_to_resource.update(
        {f"med{i}": ResourcesInternal({}, {}) for i in range(10)}
    )

    # keeps 8 of the t3.medium instances and 3 of the r6a.large instances.
    # Not sure this is actually optimal (probably not...) but it's not unreasonable.
    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, EC2_TYPE_TO_INFO, instance_to_resource
    )
    assert len(terminated_instance_ids) == 4
    assert len(list(filter(lambda ident: "lar" in ident, terminated_instance_ids))) == 2
    assert len(list(filter(lambda ident: "med" in ident, terminated_instance_ids))) == 2
    assert unmet_thresholds == []


def test_ec2_has_at_least_1GPU_10GB_available() -> None:
    thresholds = [
        Threshold(
            Resources(logical_cpu=4, memory_gb=16, gpus=1, gpu_memory=10),
            num_resources=3,
        ),
    ]

    # "g4dn.8xlarge", 1gpu 16Gb gpu_memory
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        "id0": ("r6a.large", "spot"),
        "id1": ("t3.medium", "spot"),
        "gpu": ("g4dn.8xlarge", "on_demand"),
    }
    instance_to_resource = {f"id{i}": ResourcesInternal({}, {}) for i in range(0, 2)}
    instance_to_resource["gpu"] = ResourcesInternal({}, {})

    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, EC2_TYPE_TO_INFO, instance_to_resource
    )
    assert set(terminated_instance_ids) == {"id0", "id1"}
    assert unmet_thresholds == [(thresholds[0], 2)]


def test_ec2_with_ephemeral_storage() -> None:
    thresholds = [
        Threshold(
            Resources(logical_cpu=1, memory_gb=2, ephemeral_storage_gb=100),
            num_resources=5,
        ),
    ]

    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        "id0": ("r6a.large", "spot"),  # 2 cpu, 16Gb
        "id1": ("t3.medium", "spot"),
        "gpu": ("g4dn.8xlarge", "on_demand"),
    }
    instance_to_resource = {
        "id0": ResourcesInternal({}, {EPHEMERAL_STORAGE_GB: 200}),
        "id1": ResourcesInternal({}, {EPHEMERAL_STORAGE_GB: 50}),
        "gpu": ResourcesInternal({}, {EPHEMERAL_STORAGE_GB: 250}),
    }

    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, EC2_TYPE_TO_INFO, instance_to_resource
    )
    assert set(terminated_instance_ids) == {"gpu", "id1"}
    assert unmet_thresholds == [(thresholds[0], 3)]


def test_ec2_with_custom_ami() -> None:
    thresholds = [
        Threshold(
            Resources(logical_cpu=1, memory_gb=2),
            num_resources=12,
            instance_type=AllocEC2Instance(ami_id="ami-1234456"),
        ),
    ]

    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        "id0": ("r6a.large", "spot"),
        "id1": ("t3.medium", "spot"),
        "id2": ("t3.medium", "spot"),
    }
    instance_to_resource = {
        "id0": ResourcesInternal({}, {"ami-1234456": 1.0}),
        "id1": ResourcesInternal({}, {}),
        "id2": ResourcesInternal({}, {"ami-other": 1.0}),
    }

    terminated_instance_ids, unmet_thresholds = shutdown_thresholds(
        thresholds, instances, EC2_TYPE_TO_INFO, instance_to_resource
    )
    assert set(terminated_instance_ids) == {"id1", "id2"}
    assert unmet_thresholds == [(thresholds[0], 10)]


def _mock_instance(
    mocker: MockerFixture,
    launch_time: dt.datetime,
    state: str,
    instance_type: str = "S",
) -> Instance:
    instance = mocker.Mock()
    instance.id = "0"
    instance.launch_time = launch_time
    instance.state = {"Name": state}
    instance.instance_type = instance_type
    instance.instance_lifecycle = "spot"
    return instance


def test_sync_registration_and_actual_state_unregistered(
    mocker: MockerFixture,
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    launch_register_delay = dt.timedelta(minutes=1)

    ec2_instances = {
        instance.instance_id: instance
        for instance in [
            _EC2_Instance("0", _mock_instance(mocker, now, "running"), None),
            _EC2_Instance(
                "1",
                _mock_instance(mocker, now - 2 * launch_register_delay, "running"),
                None,
            ),
        ]
    }

    sqs_client = mocker.Mock()
    _sync_registration_and_actual_state(
        "us-east-2", ec2_instances, now, launch_register_delay, sqs_client
    )

    assert ec2_instances["0"].action is not None
    assert ec2_instances["0"].reason is not None
    assert "not terminating" in ec2_instances["0"].reason

    assert ec2_instances["1"].action is not None
    assert ec2_instances["1"].reason is not None
    assert "will terminate" in ec2_instances["1"].reason


def test_sync_registration_and_actual_state_not_running(
    mocker: MockerFixture,
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    launch_register_delay = dt.timedelta(minutes=1)

    ec2_instances = {
        instance.instance_id: instance
        for instance in [
            _EC2_Instance("0", _mock_instance(mocker, now, "pending"), (now, 0)),
            _EC2_Instance(
                "1",
                None,
                (now, 0),
            ),
        ]
    }

    sqs_client = mocker.Mock()
    _sync_registration_and_actual_state(
        "us-east-2", ec2_instances, now, launch_register_delay, sqs_client
    )

    ec2_instance = ec2_instances["0"]
    assert ec2_instance.action is not None
    assert ec2_instance.reason is not None
    assert "will deregister" in ec2_instance.reason

    ec2_instance = ec2_instances["1"]
    assert ec2_instance.action is not None
    assert ec2_instance.reason is not None
    assert "will deregister" in ec2_instance.reason


def _mock_instance_2(
    mocker: MockerFixture,
    launch_time: dt.datetime,
    state: str,
    instance_type: str = "S",
) -> Instance:
    instance = mocker.Mock()
    instance.id = "0"
    instance.launch_time = launch_time
    instance.state = {"Name": state}
    instance.instance_type = instance_type
    instance.instance_lifecycle = "spot"
    instance.security_groups = []
    instance.iam_instance_profile = {"Id": "iam_instance_profile_id"}
    volumes = mocker.Mock()
    instance.volumes = volumes
    volume = mocker.Mock()
    volume.size = 100
    volumes.all = lambda: [volume]
    return instance


def test_add_deregister_and_terminate_actions(mocker: MockerFixture) -> None:
    terminate_instances_if_idle_for = dt.timedelta(minutes=1)
    launch_register_delay = dt.timedelta(minutes=1)
    now = dt.datetime.now(dt.timezone.utc)
    five_mins_ago = now - dt.timedelta(minutes=5)
    thresholds = [
        Threshold(
            Resources(logical_cpu=2, memory_gb=4, ephemeral_storage_gb=50),
            num_resources=2,
            instance_type=AllocEC2Instance(
                iam_role_instance_profile="iam_instance_profile_id"
            ),
        ),
    ]
    ec2_instances = {
        instance.instance_id: instance
        for instance in [
            # first four are deregistered/terminated right away
            _EC2_Instance("0", _mock_instance_2(mocker, now, "pending"), (now, 0)),
            _EC2_Instance(
                "1",
                None,
                (now, 0),
            ),
            _EC2_Instance("2", _mock_instance_2(mocker, now, "running"), None),
            _EC2_Instance(
                "3",
                _mock_instance_2(mocker, five_mins_ago, "running"),
                None,
            ),
            # idle, but stay alive because of thresholds
            _EC2_Instance(
                "4",
                _mock_instance_2(mocker, five_mins_ago, "running", "S"),
                (five_mins_ago, 0),
            ),
            _EC2_Instance(
                "5",
                _mock_instance_2(mocker, five_mins_ago, "running", "S"),
                (five_mins_ago, 0),
            ),
            # idle for too long, terminated
            _EC2_Instance(
                "6",
                _mock_instance_2(mocker, five_mins_ago, "running", "M"),
                (five_mins_ago, 0),
            ),
            # not idle for long enough
            _EC2_Instance(
                "7", _mock_instance_2(mocker, five_mins_ago, "running", "M"), (now, 0)
            ),
            # running a job
            _EC2_Instance(
                "8", _mock_instance_2(mocker, five_mins_ago, "running", "M"), (now, 1)
            ),
        ]
    }

    sqs_client = mocker.Mock()
    unmet_thresholds = _add_deregister_and_terminate_actions(
        "us-east-2",
        terminate_instances_if_idle_for,
        thresholds,
        launch_register_delay,
        now,
        ec2_instances,
        MOCK_TYPE_TO_INFO,
        sqs_client,
    )

    for i in range(4):
        ec2_instance = ec2_instances[str(i)]
        assert ec2_instance.action is not None
        assert ec2_instance.reason is not None
    for i in range(4, 6):
        ec2_instance = ec2_instances[str(i)]
        assert ec2_instance.action is None
        assert ec2_instance.reason is None
    ec2_instance = ec2_instances["6"]
    assert ec2_instance.action is not None
    assert ec2_instance.reason is not None
    assert "not running any jobs" in ec2_instance.reason
    for i in range(7, 9):
        ec2_instance = ec2_instances[str(i)]
        assert ec2_instance.action is None
        assert ec2_instance.reason is None
    assert unmet_thresholds == []
