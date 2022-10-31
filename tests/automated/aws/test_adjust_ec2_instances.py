from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Tuple
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
from meadowrun.config import AVX, GPU, GPU_MEMORY, LOGICAL_CPU, MEMORY_GB


if TYPE_CHECKING:
    from typing_extensions import Final
    from pytest_mock import MockerFixture
    from mypy_boto3_ec2.service_resource import Instance


MOCK_TYPE_TO_INFO: Dict[Tuple[str, OnDemandOrSpotType], CloudInstanceType] = {
    ("XS", "spot"): CloudInstanceType(
        "XS",
        "spot",
        0.05,
        ResourcesInternal(consumable={LOGICAL_CPU: 1, MEMORY_GB: 2}, non_consumable={}),
    ),
    ("S", "spot"): CloudInstanceType(
        "S",
        "spot",
        0.1,
        ResourcesInternal(consumable={LOGICAL_CPU: 2, MEMORY_GB: 4}, non_consumable={}),
    ),
    ("M", "spot"): CloudInstanceType(
        "M",
        "spot",
        0.2,
        ResourcesInternal(consumable={LOGICAL_CPU: 4, MEMORY_GB: 8}, non_consumable={}),
    ),
    ("XS-GPU", "spot"): CloudInstanceType(
        "XS-GPU",
        "spot",
        0.3,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 1, MEMORY_GB: 2, GPU: 1, GPU_MEMORY: 10},
            non_consumable={},
        ),
    ),
    ("S-GPU", "spot"): CloudInstanceType(
        "S-GPU",
        "spot",
        0.6,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 2, MEMORY_GB: 4, GPU: 2, GPU_MEMORY: 20},
            non_consumable={},
        ),
    ),
    ("M-GPU", "spot"): CloudInstanceType(
        "M-GPU",
        "spot",
        1.2,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 4, MEMORY_GB: 8, GPU: 4, GPU_MEMORY: 40},
            non_consumable={},
        ),
    ),
    ("AVX1", "spot"): CloudInstanceType(
        "XS",
        "spot",
        0.06,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 1, MEMORY_GB: 2}, non_consumable={AVX: 1.0}
        ),
    ),
    ("AVX2", "spot"): CloudInstanceType(
        "XS",
        "spot",
        0.07,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 1, MEMORY_GB: 2}, non_consumable={AVX: 2.0}
        ),
    ),
    ("AVX3", "spot"): CloudInstanceType(
        "XS",
        "spot",
        0.08,
        ResourcesInternal(
            consumable={LOGICAL_CPU: 1, MEMORY_GB: 2}, non_consumable={AVX: 3.0}
        ),
    ),
}


def test_shutdown_thresholds_single_threshold() -> None:
    thresholds = [Threshold(times=1, logical_cpu_count=2, memory_gb=4)]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {"id1": ("S", "spot")}

    # S instance matches exactly with threshold
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert actual_instance_ids == []

    # an extra S instance, one of them should shut down
    instances["id2"] = ("S", "spot")
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert len(actual_instance_ids) == 1

    # an extra XS instance, too small for threshold.
    # Should shut down, as well as one of id1 or id2.
    instances["id3"] = ("XS", "spot")
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert len(actual_instance_ids) == 2
    assert "id3" in actual_instance_ids


def test_shutdown_thresholds_single_threshold_resource_too_small() -> None:
    # XS instance is too small for threshold, should shut down
    thresholds = [Threshold(times=1, logical_cpu_count=2, memory_gb=4)]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {"id1": ("XS", "spot")}
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert actual_instance_ids == ["id1"]


def test_shutdown_thresholds_single_threshold_resource_bigger() -> None:
    # M instance is bigger than threshold, should keep it
    thresholds = [Threshold(times=1, logical_cpu_count=2, memory_gb=4)]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {"id1": ("M", "spot")}
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert actual_instance_ids == []

    # id2 is S instance, cheaper to keep this one.
    instances["id2"] = ("S", "spot")
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert actual_instance_ids == ["id1"]


def test_shutdown_thresholds_single_threshold_keep_cheapest_resource() -> None:
    # M instance is bigger than threshold, should keep it
    thresholds = [Threshold(times=1, logical_cpu_count=1, memory_gb=2)]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        "id1": ("S", "spot"),
        "id2": ("XS", "spot"),
        "id3": ("M", "spot"),
        "id4": ("S", "spot"),
    }
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert set(actual_instance_ids) == set(["id1", "id3", "id4"])


def test_shutdown_thresholds_single_threshold_resource_has_gpu() -> None:
    # S-GPU has the right size for threshold, but has GPU. Should shut down.
    thresholds = [Threshold(times=1, logical_cpu_count=2, memory_gb=4)]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {"id1": ("S-GPU", "spot")}
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert actual_instance_ids == ["id1"]

    # S instance has right size. Should not shut down.
    instances["id2"] = ("S", "spot")
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert actual_instance_ids == ["id1"]


def test_shutdown_thresholds_single_threshold_multiple_times() -> None:
    thresholds = [Threshold(times=4, logical_cpu_count=2, memory_gb=4)]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        f"id{i}": ("S", "spot") for i in range(4)
    }

    # S instance matches exactly with threshold
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert actual_instance_ids == []

    # an extra S instance, one of them should shut down
    instances["id4"] = ("S", "spot")
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert len(actual_instance_ids) == 1

    # an extra M instance, one of them should shut down
    instances["id5"] = ("M", "spot")
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert len(actual_instance_ids) == 2
    assert "id5" in actual_instance_ids

    # an extra XS instance, too small for threshold.
    # Should shut down, as well as one of id1 or id2.
    instances["id6"] = ("XS", "spot")
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert len(actual_instance_ids) == 3
    assert "id5" in actual_instance_ids
    assert "id6" in actual_instance_ids


def test_shutdown_thresholds_multiple_thresholds() -> None:
    thresholds = [
        Threshold(times=1, logical_cpu_count=2, memory_gb=4),
        Threshold(times=1, gpu_count=1, gpu_memory_gb=10),
    ]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        "id0": ("S", "spot"),
        "id1": ("XS-GPU", "spot"),
    }

    # S/XS-GPU instance matches exactly with one threshold each
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert actual_instance_ids == []

    # an extra S instance, one of them should shut down
    instances["id2"] = ("S", "spot")
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert len(actual_instance_ids) == 1
    assert "id1" not in actual_instance_ids

    # an extra S-GPU instance, one of them should shut down
    instances["id3"] = ("S-GPU", "spot")
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert len(actual_instance_ids) == 2
    assert "id3" in actual_instance_ids

    # an extra XS instance, too small for threshold.
    # Should shut down, as well as one of id1 or id2.
    instances["id4"] = ("XS", "spot")
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert len(actual_instance_ids) == 3
    assert "id3" in actual_instance_ids
    assert "id4" in actual_instance_ids


def test_shutdown_thresholds_multiple_thresholds_multiple_times() -> None:
    thresholds = [
        Threshold(times=5, logical_cpu_count=2, memory_gb=4),
        Threshold(times=2, gpu_count=1, gpu_memory_gb=10),
    ]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        f"id{i}": ("S", "spot") for i in range(4)
    }
    instances.update({f"gpu-id{i}": ("XS-GPU", "spot") for i in range(1)})

    # not enough resources to match threshold
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert actual_instance_ids == []

    # an extra S instance, should be shut down as it's too small
    instances["id4"] = ("XS", "spot")
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert actual_instance_ids == ["id4"]

    # an extra S instance, matches first threshold exactly
    instances["id5"] = ("S", "spot")
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert actual_instance_ids == ["id4"]

    # an extra GPU instance, should shut down the smaller GPU machine
    instances["gpu-id1"] = ("M-GPU", "spot")
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert set(actual_instance_ids) == {"id4", "gpu-id0"}


def test_shutdown_thresholds_second_pass() -> None:
    thresholds = [
        Threshold(times=5, logical_cpu_count=2, memory_gb=4),
    ]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        f"id{i}": ("S", "spot") for i in range(4)
    }
    instances["big"] = ("M", "spot")

    # should keep the M instance, and just enough of the smaller ones to reach threshold
    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert len(actual_instance_ids) == 1
    assert "big" not in actual_instance_ids

    # this doesn't work, because big2 is not even assigned in the first pass, so the
    # second pass can't remove it.
    # instances["big2"] = ("M", "spot")

    # # can shut down two more of the S instances, keep the two M.
    # actual_instance_ids = shutdown_thresholds(thresholds, instances,MOCK_TYPE_TO_INFO)
    # assert len(actual_instance_ids) == 3
    # assert "big" not in actual_instance_ids
    # assert "big2" not in actual_instance_ids


def test_shutdown_thresholds_flags() -> None:
    thresholds = [
        Threshold(times=2, logical_cpu_count=1, memory_gb=2, flags=frozenset(["AVX2"])),
    ]
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        "id0": ("AVX1", "spot"),
        "id1": ("AVX2", "spot"),
        "id2": ("AVX3", "spot"),
        "id3": ("AVX1", "spot"),
    }

    actual_instance_ids = shutdown_thresholds(thresholds, instances, MOCK_TYPE_TO_INFO)
    assert set(actual_instance_ids) == {"id0", "id3"}


def _get_ec2_type_info_snapshot() -> Dict[
    Tuple[str, OnDemandOrSpotType], CloudInstanceType
]:
    #
    # Path(__file__).parent /

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
        Threshold(times=20, logical_cpu_count=1, memory_gb=4),
    ]

    # 2cpu 16Gb
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        f"lar{i}": ("r6a.large", "spot") for i in range(5)
    }
    # 2cpu 4Gb
    instances.update({f"med{i}": ("t3.medium", "spot") for i in range(10)})

    # keeps 8 of the t3.medium instances and 3 of the r6a.large instances.
    # Not sure this is actually optimal (probably not...) but it's not unreasonable.
    actual_instance_ids = shutdown_thresholds(thresholds, instances, EC2_TYPE_TO_INFO)
    assert len(actual_instance_ids) == 4
    assert len(list(filter(lambda ident: "lar" in ident, actual_instance_ids))) == 2
    assert len(list(filter(lambda ident: "med" in ident, actual_instance_ids))) == 2


def test_ec2_has_at_least_1GPU_10GB_available() -> None:
    thresholds = [
        Threshold(
            times=3, logical_cpu_count=4, memory_gb=16, gpu_count=1, gpu_memory_gb=10
        ),
    ]

    # "g4dn.8xlarge", 1gpu 16Gb gpu_memory
    instances: Dict[str, Tuple[str, OnDemandOrSpotType]] = {
        "id0": ("r6a.large", "spot"),
        "id1": ("t3.medium", "spot"),
        "gpu": ("g4dn.8xlarge", "on_demand"),
    }

    actual_instance_ids = shutdown_thresholds(thresholds, instances, EC2_TYPE_TO_INFO)
    assert set(actual_instance_ids) == {"id0", "id1"}


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

    _sync_registration_and_actual_state(
        "us-east-2",
        ec2_instances,
        now,
        launch_register_delay,
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

    _sync_registration_and_actual_state(
        "us-east-2",
        ec2_instances,
        now,
        launch_register_delay,
    )

    ec2_instance = ec2_instances["0"]
    assert ec2_instance.action is not None
    assert ec2_instance.reason is not None
    assert "will deregister" in ec2_instance.reason

    ec2_instance = ec2_instances["1"]
    assert ec2_instance.action is not None
    assert ec2_instance.reason is not None
    assert "will deregister" in ec2_instance.reason


def test__add_deregister_and_terminate_actions(mocker: MockerFixture) -> None:
    terminate_instances_if_idle_for = dt.timedelta(minutes=1)
    launch_register_delay = dt.timedelta(minutes=1)
    now = dt.datetime.now(dt.timezone.utc)
    five_mins_ago = now - dt.timedelta(minutes=5)
    thresholds = [
        Threshold(times=2, logical_cpu_count=2, memory_gb=4),
    ]
    ec2_instances = {
        instance.instance_id: instance
        for instance in [
            # first four are deregistered/terminated right away
            _EC2_Instance("0", _mock_instance(mocker, now, "pending"), (now, 0)),
            _EC2_Instance(
                "1",
                None,
                (now, 0),
            ),
            _EC2_Instance("2", _mock_instance(mocker, now, "running"), None),
            _EC2_Instance(
                "3",
                _mock_instance(mocker, five_mins_ago, "running"),
                None,
            ),
            # idle, but stay alive because of thresholds
            _EC2_Instance(
                "4",
                _mock_instance(mocker, five_mins_ago, "running", "S"),
                (five_mins_ago, 0),
            ),
            _EC2_Instance(
                "5",
                _mock_instance(mocker, five_mins_ago, "running", "S"),
                (five_mins_ago, 0),
            ),
            # idle for too long, terminated
            _EC2_Instance(
                "6",
                _mock_instance(mocker, five_mins_ago, "running", "M"),
                (five_mins_ago, 0),
            ),
            # not idle for long enough
            _EC2_Instance(
                "7", _mock_instance(mocker, five_mins_ago, "running", "M"), (now, 0)
            ),
            # running a job
            _EC2_Instance(
                "8", _mock_instance(mocker, five_mins_ago, "running", "M"), (now, 1)
            ),
        ]
    }
    _add_deregister_and_terminate_actions(
        "us-east-2",
        terminate_instances_if_idle_for,
        thresholds,
        launch_register_delay,
        now,
        ec2_instances,
        MOCK_TYPE_TO_INFO,
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
