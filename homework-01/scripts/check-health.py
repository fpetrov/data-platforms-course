#!/usr/bin/env python3
import json
import sys
from datetime import datetime, timezone
from urllib.request import urlopen

nn, snn, *expected_ips = sys.argv[1:]


def beans(host, port):
    with urlopen(f"http://{host}:{port}/jmx", timeout=5) as response:
        return {bean["name"].split("name=")[-1]: bean for bean in json.load(response)["beans"]}


try:
    nn_beans = beans(nn, 9870)
    state = nn_beans["FSNamesystemState"]
    filesystem = nn_beans["FSNamesystem"]
    info = nn_beans["NameNodeInfo"]
    assert state["NumLiveDataNodes"] == 3, "Ожидались 3 живых DataNode"
    for metric in (
        "NumDeadDataNodes", "NumStaleDataNodes", "NumStaleStorages",
        "NumDecommissioningDataNodes", "NumDecomLiveDataNodes", "NumDecomDeadDataNodes",
        "NumInMaintenanceLiveDataNodes", "NumInMaintenanceDeadDataNodes",
        "NumEnteringMaintenanceDataNodes", "VolumeFailuresTotal",
    ):
        assert state[metric] == 0, f"{metric}={state[metric]}"
    for metric in ("MissingBlocks", "CorruptBlocks", "UnderReplicatedBlocks"):
        assert filesystem[metric] == 0, f"{metric}={filesystem[metric]}"
    assert not info["Safemode"], "NameNode в safe mode"
    nodes = json.loads(info["LiveNodes"])
    actual_ips = {node["xferaddr"].split(":")[0] for node in nodes.values()}
    assert actual_ips == set(expected_ips), f"Неожиданный состав DataNode: {actual_ips}"
    assert all(node["adminState"] == "In Service" for node in nodes.values())
    checkpoint = beans(snn, 9868)["SecondaryNameNodeInfo"]["LastCheckpointTime"]
    assert checkpoint > 0, "SecondaryNameNode еще не создал checkpoint"
    print("DataNode: 3 живых, dead/stale/decommission/maintenance: 0, сбои дисков: 0")
    print("Блоки: missing/corrupt/under-replicated: 0, safe mode: OFF")
    print("Checkpoint:", datetime.fromtimestamp(checkpoint / 1000, timezone.utc).isoformat())
except (AssertionError, KeyError, OSError, ValueError) as error:
    print(f"Проверка HDFS не прошла: {error}", file=sys.stderr)
    sys.exit(1)
