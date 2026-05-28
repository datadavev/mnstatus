import csv
import json
import logging
import logging.handlers
import os
import sys
import typing

import click
import geojson

import mnstatus

LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "FATAL": logging.CRITICAL,
    "CRITICAL": logging.CRITICAL,
}

# Empty unless needed
W = ""  # white (normal)
R = ""  # red
G = ""  # green
O = ""  # orange
B = ""  # blue
P = ""  # purple

PRODUCTION_CN = "https://cn.dataone.org/cn"


def getLogFormatter():
    return logging.Formatter("%(asctime)-8s %(levelname)-6s: %(message)-s", "%H:%M:%S")


def stateInt(state):
    if state.lower() == "up":
        return 1
    return 0


def find_entry_by_nodeid(
    nodeid: str, data: list[dict[typing.Any, typing.Any]]
) -> dict[typing.Any, typing.Any] | None:
    for record in data:
        if record.get("identifier") == nodeid:
            return record
    return None


@click.group()
@click.option(
    "--verbosity",
    envvar="VERBOSITY",
    default="INFO",
    help="Specify logging level",
    show_default=True,
)
@click.option("-J", "--json", "json_format", is_flag=True, help="Output in JSON")
@click.option(
    "-C",
    "--cnode_url",
    envvar="CNODE_URL",
    default=PRODUCTION_CN,
    help="Base URL of Coordinating Node",
)
@click.option(
    "--terminal_colors",
    envvar="TERMINAL_COLORS",
    is_flag=True,
    help="Use colors in terminal",
)
@click.option(
    "--solr_url",
    envvar="CN_SOLR_URL",
    default=mnstatus.SOLR_URL,
    help="Solr URL absolute or relative to CN base URL.",
)
@click.pass_context
def main(ctx, verbosity, json_format, cnode_url, terminal_colors, solr_url):
    logger = mnstatus.getLogger()
    logger.setLevel(LOG_LEVELS.get(verbosity.upper(), logging.INFO))
    c_handler = logging.StreamHandler()
    c_handler.setFormatter(getLogFormatter())
    logger.addHandler(c_handler)
    ctx.ensure_object(dict)
    ctx.obj["json_format"] = json_format
    ctx.obj["cnode_url"] = cnode_url
    ctx.obj["solr_url"] = solr_url
    if terminal_colors:
        global W, R, G, O, B, P
        W = "\033[0m"  # white (normal)
        R = "\033[31m"  # red
        G = "\033[32m"  # green
        O = "\033[33m"  # orange
        B = "\033[34m"  # blue
        P = "\033[35m"  # purple
    return 0


@main.command("nids", short_help="List nodes from CN node list")
@click.option("-n", "--n_type", default=None, help="Specify node type, mn or cn")
@click.option(
    "-s", "--state", "n_state", default=None, help="Specify node state, up or down"
)
@click.option("-F", "--full", "show_full", is_flag=True, help="Show full node records")
@click.option(
    "-t",
    "--test",
    "tests",
    multiple=True,
    default=[],
    help="Tests to run",
)
@click.option(
    "-T",
    "--timeout",
    envvar="HTTP_TIMEOUT",
    default=mnstatus.HTTP_TIMEOUT,
    help="HTTP connection timeout in seconds",
)
@click.pass_context
def listNodes(ctx, n_type, n_state, show_full, tests, timeout):
    _L = mnstatus.getLogger()
    if not n_type is None:
        n_type = n_type.lower()
    if n_type not in [None, "cn", "mn"]:
        _L.error("Expecting 'mn' or 'cn' for the node type")
        return 1
    if not n_state is None:
        n_state = n_state.lower()
    if n_state not in [None, "up", "down"]:
        _L.error("Expecting node state to be 'up' or 'down'")
        return 1
    cn = mnstatus.NodeList(base_url=ctx.obj["cnode_url"])
    cn.filterNodeState(n_state)
    cn.filterNodeType(n_type)
    if len(tests) > 0:
        cn.testNodeConnectivity(tests, solr_url=ctx.obj["solr_url"], timeout=timeout)
    if ctx.obj.get("json_format", False):
        if show_full:
            print(mnstatus.jsonDumps(cn.nodes()))
        else:
            res = []
            for n in cn.nodes():
                entry = {
                    "identifier": n["identifier"],
                    "baseURL": n["baseURL"],
                    "name": n["name"],
                    "@state": n["@state"],
                    "@type": n["@type"],
                    "lastHarvested": n.get("synchronization", {}).get(
                        "lastHarvested", ""
                    ),
                }
                if "status" in n.keys():
                    if "ping" in n["status"]:
                        entry["ping"] = {
                            "status": n["status"]["ping"]["status"],
                            "msg": n["status"]["ping"]["message"],
                        }
                    if "mn" in n["status"]:
                        entry["mn"] = {
                            "count": n["status"]["mn"]["count"],
                            "latest": n["status"]["mn"]["latest"],
                        }
                    if "cn" in n["status"]:
                        entry["cn"] = {
                            "count": n["status"]["cn"]["count"],
                            "latest": n["status"]["cn"]["latest"],
                        }
                    if "index" in n["status"]:
                        entry["index"] = {
                            "count": n["status"]["index"]["count"],
                            "latest": n["status"]["index"]["latest"],
                        }
                res.append(entry)
            print(mnstatus.jsonDumps(res))
        return 0
    for n in cn.nodes():
        lh = n.get("synchronization", {}).get("lastHarvested", "")
        _ping = n.get("status", {}).get("ping", {}).get("status")
        print(
            f"{n['identifier']:25} {n['@type']} {stateInt(n['@state'])} {n['baseURL']:55} {lh} {_ping}"
        )
    return 0


@main.command("node", short_help="Check node status")
@click.argument("node_id")
@click.option(
    "-T",
    "--timeout",
    envvar="HTTP_TIMEOUT",
    default=mnstatus.HTTP_TIMEOUT,
    help="HTTP connection timeout in seconds",
)
@click.option(
    "-t",
    "--test",
    "tests",
    multiple=True,
    default=["ping", "mn", "cn", "index"],
    help="Tests to run",
)
@click.pass_context
def checkNode(ctx, node_id, timeout, tests):
    _L = mnstatus.getLogger()
    tests = list(tests)
    cn = mnstatus.NodeList(base_url=ctx.obj["cnode_url"])
    if node_id.startswith("http"):
        node_id = cn.nodeId(node_id)
    cn.testNodeConnectivity(
        tests,
        solr_url=ctx.obj["solr_url"],
        timeout=timeout,
        node_ids_to_test=[
            node_id,
        ],
    )
    mn = cn.node(node_id)
    print(mnstatus.jsonDumps(mn["status"]))
    return 0


@main.command("geojson", short_help="Generate nodes GeoJSON")
@click.option("-n", "--n_type", default=None, help="Specify node type, mn or cn")
@click.option(
    "-s", "--state", "n_state", default=None, help="Specify node state, up or down"
)
@click.option(
    "-i", "--status_info", default=None, help="Path to JSON report data to include"
)
@click.pass_context
def generate_geojson(
    ctx, n_type: str | None, n_state: str | None, status_info: str | None
):
    _L = mnstatus.getLogger()
    if n_type is not None:
        n_type = n_type.lower()
    if n_type not in [None, "cn", "mn"]:
        _L.error("Expecting 'mn' or 'cn' for the node type")
        return 1
    if n_state is not None:
        n_state = n_state.lower()
    if n_state not in [None, "up", "down"]:
        _L.error("Expecting node state to be 'up' or 'down'")
        return 1
    status_data = None
    if status_info is not None:
        with open(status_info, "r") as fsrc:
            status_data = json.load(fsrc)

    cn = mnstatus.NodeList(base_url=ctx.obj["cnode_url"])
    cn.filterNodeState(n_state)
    cn.filterNodeType(n_type)
    data = cn.getDisplayInfo()
    features = []
    for k, v in data.items():
        if len(v["location"]) == 2:
            _properties = v["properties"]
            if status_data is not None:
                _metrics = find_entry_by_nodeid(k, status_data)
                if _metrics is not None:
                    stats = _metrics.get("status", {})
                    _properties["mn_http_status"] = stats.get("mn", {}).get(
                        "status", -999
                    )
                    _properties["mn_http_elapsed"] = stats.get("ping", {}).get(
                        "elapsed", -999
                    )
                    _properties["mn_count"] = stats.get("mn", {}).get("count", 0)
                    _properties["mn_earliest"] = stats.get("mn", {}).get(
                        "earliest", "1900-01-01T00:00:00.000+00:00"
                    )
                    _properties["mn_latest"] = stats.get("mn", {}).get(
                        "latest", "1900-01-01T00:00:00.000+00:00"
                    )
                    _properties["cn_count"] = stats.get("cn", {}).get("count", 0)
                    _properties["cn_earliest"] = stats.get("cn", {}).get(
                        "earliest", "1900-01-01T00:00:00.000+00:00"
                    )
                    _properties["cn_latest"] = stats.get("cn", {}).get(
                        "latest", "1900-01-01T00:00:00.000+00:00"
                    )
                    _properties["index_count"] = stats.get("index", {}).get("count", 0)
                    _properties["index_earliest"] = stats.get("index", {}).get(
                        "earliest", "1900-01-01T00:00:00.000+00:00"
                    )
                    _properties["index_latest"] = stats.get("index", {}).get(
                        "latest", "1900-01-01T00:00:00.000+00:00"
                    )
            feature = geojson.Feature(
                id=k,
                geometry=geojson.Point(v["location"]),
                properties=_properties,
            )
            features.append(feature)
    feature_collection = geojson.FeatureCollection(features)
    print(json.dumps(feature_collection, indent=2))


@main.command("2csv", short_help="JSON report to CSV")
@click.argument("source")
@click.option("-o", "--output", default="-", help="Output file (stdout default)")
@click.pass_context
def reportJson2CSV(ctx, source, output):
    """Convert the JSON report to CSV."""
    _L = mnstatus.getLogger()
    if not os.path.exists(source):
        _L.error("Source does not exist: %s", source)
        return 1
    data = None
    with open(source, "r") as fsrc:
        data = json.load(fsrc)
    header = [
        "node_id",
        "baseurl",
        "state",
        "sync",
        "status",
        "mn.count",
        "mn.elapsed",
        "mn.earliest",
        "mn.latest",
        "cn.count",
        "cn.elapsed",
        "cn.earliest",
        "cn.latest",
        "idx.count",
        "idx.elapsed",
        "idx.earliest",
        "idx.latest",
        "tstamp",
    ]
    f_dest = None
    _doclose = True
    if output == "-":
        f_dest = sys.stdout
        _doclose = False
    else:
        f_dest = open(output, "w")
    try:
        csvout = csv.DictWriter(f_dest, fieldnames=header, extrasaction="ignore")
        csvout.writeheader()
        for node in data:
            if node.get("@type", "").lower() == "mn":
                status = node.get("status", {})
                row = {
                    "node_id": node.get("identifier"),
                    "baseurl": node.get("baseURL"),
                    "state": node.get("@state"),
                    "sync": node.get("@synchronize"),
                    "status": status.get("ping", {}).get("status"),
                    "mn.count": status.get("mn", {}).get("count"),
                    "mn.elapsed": status.get("mn", {}).get("elapsed"),
                    "mn.earliest": status.get("mn", {}).get("earliest"),
                    "mn.latest": status.get("mn", {}).get("latest"),
                    "cn.count": status.get("cn", {}).get("count"),
                    "cn.elapsed": status.get("cn", {}).get("elapsed"),
                    "cn.earliest": status.get("cn", {}).get("earliest"),
                    "cn.latest": status.get("cn", {}).get("latest"),
                    "idx.count": status.get("index", {}).get("count"),
                    "idx.elapsed": status.get("index", {}).get("elapsed"),
                    "idx.earliest": status.get("index", {}).get("earliest"),
                    "idx.latest": status.get("index", {}).get("latest"),
                    "tstamp": status.get("ping", {}).get("tstamp"),
                }
                csvout.writerow(row)
    finally:
        if _doclose:
            f_dest.close()


if __name__ == "__main__":
    sys.exit(main())
