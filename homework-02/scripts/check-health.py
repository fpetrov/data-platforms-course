#!/usr/bin/env python3
import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


def check(condition, message):
    if not condition:
        raise ValueError(message)


def request(url, method="GET", status=200):
    try:
        with urlopen(Request(url, method=method), timeout=10) as response:
            code, data, final_url = response.status, response.read(), response.url
    except HTTPError as error:
        code, data, final_url = error.code, error.read(), error.url
    check(code == status, f"{method} {url}: HTTP {code}, ожидался {status}")
    return data, final_url


def api(host, port, path):
    return json.loads(request(f"http://{host}:{port}{path}")[0])


def command(args):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT)


def remote(host, program):
    return command([
        "sudo", "-u", "hadoop", "ssh", "-n", "-i", env["HADOOP_KEY"],
        "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o",
        f"UserKnownHostsFile={env['HADOOP_KNOWN_HOSTS']}", host, program,
    ])


def health(snapshot):
    info = api(edge, 8088, "/ws/v1/cluster/info")["clusterInfo"]
    check(info["state"] == "STARTED", "ResourceManager не запущен")
    metrics = api(edge, 8088, "/ws/v1/cluster/metrics")["clusterMetrics"]
    check(metrics["activeNodes"] == metrics["totalNodes"] == 3, "Ожидались 3 активных NM")
    for key in ("lostNodes", "unhealthyNodes", "decommissioningNodes", "decommissionedNodes", "rebootedNodes", "shutdownNodes"):
        check(metrics[key] == 0, f"{key}={metrics[key]}")
    check(metrics["totalMB"] == 3 * int(env["NODE_MEMORY_MB"]), "Неверная память YARN")
    check(metrics["totalVirtualCores"] == 3 * int(env["NODE_VCORES"]), "Неверное число vcores")
    nodes = api(edge, 8088, "/ws/v1/cluster/nodes")["nodes"]["node"]
    check(len(nodes) == 3 and {node["nodeHostName"] for node in nodes} == set(workers), "Неожиданный состав NM")
    for node in nodes:
        check(node["state"] == "RUNNING" and not node["healthReport"], f"NM {node['nodeHostName']}: {node}")
        check(node["usedMemoryMB"] + node["availMemoryMB"] == int(env["NODE_MEMORY_MB"]), "Неверная память NM")
        check(node["usedVirtualCores"] + node["availableVirtualCores"] == int(env["NODE_VCORES"]), "Неверные vcores NM")
        nm = api(node["nodeHostName"], 8042, "/ws/v1/node/info")["nodeInfo"]
        check(nm["nodeHealthy"] and not nm["healthReport"], f"NM {node['nodeHostName']} нездоров")
    check(api(edge, 19888, "/ws/v1/history/info")["historyInfo"]["startedOn"] > 0, "JHS не запущен")
    check(api(edge, 8188, "/ws/v1/timeline/")["About"] == "Timeline API", "ATS не запущен")
    proxy = api(edge, 9099, "/jmx")["beans"]
    check(any(bean["name"] == "java.lang:type=Runtime" for bean in proxy), "Нет JMX WebAppProxy")
    expected = [
        {"ResourceManager", "JobHistoryServer", "ApplicationHistoryServer", "WebAppProxyServer"},
        {"NameNode", "DataNode", "NodeManager"},
        {"DataNode", "NodeManager"},
        {"SecondaryNameNode", "DataNode", "NodeManager"},
    ]
    hdfs_processes = {}
    for host, classes in zip([edge, *workers], expected):
        program = "/usr/lib/jvm/java-11-openjdk-amd64/bin/jps -l"
        output = command(["sudo", "-u", "hadoop", *program.split()]) if host == edge else remote(host, program)
        processes = [line.split() for line in output.splitlines() if "org.apache.hadoop." in line]
        check(len(processes) == len(classes) and {item[1].split(".")[-1] for item in processes} == classes,
              f"Неверные процессы на {host}: {output}")
        hdfs_processes[host] = sorted(item for item in processes if ".hdfs." in item[1])
        print(f"{host}: {', '.join(sorted(classes))}")
    if snapshot:
        path = Path(snapshot)
        if path.exists():
            check(json.loads(path.read_text()) == hdfs_processes, "HDFS был перезапущен вместе с YARN")
            print("Процессы HDFS сохранили PID.")
        else:
            path.write_text(json.dumps(hdfs_processes))
    print(f"YARN: 3 здоровых NM, {metrics['totalMB']} МиБ, {metrics['totalVirtualCores']} vcores")


def daemon_logs():
    script = r"""import pathlib, re, sys
root = pathlib.Path(sys.argv[1])
files = sorted(root.glob('*.log'))
if not files:
    raise SystemExit('Нет логов демонов')
for path in files:
    lines = path.read_text(errors='replace').splitlines()
    starts = [i for i, line in enumerate(lines) if 'STARTUP_MSG: Starting ' in line]
    if not starts:
        raise SystemExit(f'Нет STARTUP_MSG в {path}')
    errors = [line for line in lines[starts[-1]:] if re.search(r' (ERROR|FATAL) ', line)]
    if errors:
        raise SystemExit('\n'.join(errors))
print(f'{len(files)} текущих логов: ERROR/FATAL нет')
"""
    for host in [edge, *workers]:
        if host == edge:
            output = command(["sudo", "-u", "hadoop", "python3", "-c", script, env["LOG_DIR"]])
        else:
            output = remote(host, f"python3 -c {shlex.quote(script)} {shlex.quote(env['LOG_DIR'])}")
        print(f"{host}: {output.strip()}")


def job(app_id, job_id, restarted):
    if not restarted:
        app = api(edge, 8088, f"/ws/v1/cluster/apps/{app_id}")["app"]
        check(app["state"] == "FINISHED" and app["finalStatus"] == "SUCCEEDED", f"YARN: {app}")
        check(app["applicationType"] == "MAPREDUCE", "Приложение не MapReduce")
        check(app["logAggregationStatus"] == "SUCCEEDED", f"Логи: {app['logAggregationStatus']}")
        print(f"YARN: {app_id}, FINISHED/SUCCEEDED, MAPREDUCE, логи SUCCEEDED")
    history = api(edge, 19888, f"/ws/v1/history/mapreduce/jobs/{job_id}")["job"]
    check(history["id"] == job_id and history["state"] == "SUCCEEDED", "Задание не найдено в JHS")
    check(history["mapsTotal"] == history["mapsCompleted"] == 2, "Ожидались 2 завершенных map")
    check(history["reducesTotal"] == history["reducesCompleted"] == 1, "Ожидался 1 завершенный reduce")
    check(history["uberized"] is False, "Задание исполнилось в uber mode")
    check(history["failedMapAttempts"] == history["failedReduceAttempts"] == 0, "Есть неудачные попытки задач")
    timeline = api(edge, 8188, f"/ws/v1/timeline/YARN_APPLICATION/{app_id}")
    check(timeline["entity"] == app_id and timeline["entitytype"] == "YARN_APPLICATION", "Нет приложения в ATS")
    events = {event["eventtype"]: event for event in timeline["events"]}
    check("YARN_APPLICATION_FINISHED" in events, "ATS не сохранил событие завершения")
    final = events["YARN_APPLICATION_FINISHED"]["eventinfo"]
    check(final["YARN_APPLICATION_FINAL_STATUS"] == "SUCCEEDED", f"ATS: {final}")
    print(f"JHS: {job_id}, SUCCEEDED, map 2/2, reduce 1/1, uber=false")
    print(f"ATS: {app_id}, YARN_APPLICATION_FINISHED/SUCCEEDED")


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.assets = []
        self.ports = set()

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if "data-port" in attrs:
            self.ports.add(int(attrs["data-port"]))
        if tag == "script" and "src" in attrs:
            self.assets.append(attrs["src"])
        if tag == "link" and attrs.get("rel") == "stylesheet" and "href" in attrs:
            self.assets.append(attrs["href"])


def web():
    pages = [(19870, "/dfshealth.html"), (19868, "/status.html"), (19864, "/datanode.html"),
             (19865, "/datanode.html"), (19866, "/datanode.html"), (18088, "/cluster"),
             (18042, "/node"), (18043, "/node"), (18044, "/node"), (29888, "/jobhistory"),
             (18188, "/applicationhistory"), (19099, "/jmx")]
    ports = {18080, *(port for port, _ in pages)}
    listeners = {line.split()[3] for line in command(["ss", "-ltnH"]).splitlines()
                 if int(line.split()[3].rsplit(":", 1)[1]) in ports}
    check(listeners == {f"127.0.0.1:{port}" for port in ports}, "Веб-порты должны слушать только localhost")
    index = request("http://127.0.0.1:18080/")[0].decode()
    parser = Links()
    parser.feed(index)
    check(parser.ports == {port for port, _ in pages}, "В указателе нет всех 12 интерфейсов")
    assets = set()
    for port, path in pages:
        url = f"http://127.0.0.1:{port}{path}"
        data, final_url = request(url)
        check(urlparse(final_url).hostname == "127.0.0.1", f"Внутренний redirect: {final_url}")
        if path != "/jmx":
            page = data.decode()
            check("<html" in page.lower(), f"Не HTML: {url}")
            parser = Links()
            parser.feed(page)
            for asset in parser.assets:
                target = urljoin(final_url, asset)
                if urlparse(target).hostname == "127.0.0.1":
                    assets.add(target)
        request(f"http://127.0.0.1:{port}/", method="POST", status=405)
        request(f"http://127.0.0.1:{port}/logLevel", status=403)
        request(f"http://127.0.0.1:{port}/prof", status=403)
        request(f"http://127.0.0.1:{port}/?op=GETDELEGATIONTOKEN", status=403)
    for target in sorted(assets):
        check(request(target)[0], f"Пустой ресурс: {target}")
    request("http://127.0.0.1:18080/", method="POST", status=405)
    rm = api("127.0.0.1", 18088, "/ws/v1/cluster/info")["clusterInfo"]
    check(rm["state"] == "STARTED", "RM REST через nginx не работает")
    published_nodes = api("127.0.0.1", 18088, "/ws/v1/cluster/nodes")["nodes"]["node"]
    check({node["nodeHTTPAddress"] for node in published_nodes} == {f"127.0.0.1:{port}" for port in (18042, 18043, 18044)},
          "Ссылки RM на NM не переписаны")
    for port in (18042, 18043, 18044):
        check(api("127.0.0.1", port, "/ws/v1/node/info")["nodeInfo"]["nodeHealthy"], "NM REST через nginx не работает")
    check(api("127.0.0.1", 29888, "/ws/v1/history/info")["historyInfo"]["startedOn"] > 0, "JHS REST через nginx не работает")
    check(api("127.0.0.1", 18188, "/ws/v1/timeline/")["About"] == "Timeline API", "ATS REST через nginx не работает")
    for port in (19870, 19868, 19864, 19865, 19866, 19099):
        check(api("127.0.0.1", port, "/jmx")["beans"], f"JMX через {port} не работает")
    nn = api("127.0.0.1", 19870, "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo")["beans"][0]
    addresses = {node["infoAddr"] for node in json.loads(nn["LiveNodes"]).values()}
    check(addresses == {f"127.0.0.1:{port}" for port in (19864, 19865, 19866)}, "Ссылки NN на DN не переписаны")
    print(f"Веб: указатель, 12 интерфейсов, {len(assets)} CSS/JS, REST/JMX и ссылки доступны")
    print("Веб: все 13 портов слушают только 127.0.0.1")
    print("Веб: POST=405, logLevel/prof/delegation token=403")


def logs(path):
    text = Path(path).read_text()
    containers = set(re.findall(r"^Container: (container_\S+)", text, re.MULTILINE))
    check(len(containers) >= 4, f"Ожидались логи AM, 2 map и reduce, найдено {len(containers)}")
    check("LogType:syslog" in text and "LogType:stdout" in text, "Нет syslog/stdout контейнеров")
    check("MRAppMaster" in text and "MapTask" in text and "ReduceTask" in text, "Нет содержимого AM/map/reduce")
    print(f"Агрегированные логи: {len(containers)} контейнера, stdout/syslog читаются")


env = os.environ
edge = env["EDGE_HOST"]
workers = [env[key] for key in ("NAMENODE_HOST", "DATANODE0_HOST", "DATANODE1_HOST")]
parser = argparse.ArgumentParser()
parser.add_argument("mode", choices=("health", "daemon-logs", "job", "web", "logs"))
parser.add_argument("arguments", nargs="*")
parser.add_argument("--restarted", action="store_true")
args = parser.parse_args()
try:
    if args.mode == "health":
        health(args.arguments[0] if args.arguments else None)
    elif args.mode == "daemon-logs":
        daemon_logs()
    elif args.mode == "job":
        job(*args.arguments, args.restarted)
    elif args.mode == "web":
        web()
    else:
        logs(args.arguments[0])
except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
    print(f"Проверка не прошла: {error}", file=sys.stderr)
    sys.exit(1)
