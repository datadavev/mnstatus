import sys
import os
import logging
import logging.handlers
import multiprocessing
import click
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
    "-S",
    "--sort",
    "sort_by",
    default="UNSORTED",
    type=click.Choice(["UNSORTED", "ID", "DATE"], case_sensitive=False),
)
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
def listNodes(ctx, n_type, n_state, show_full, sort_by, tests, timeout):
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
    if not sort_by == "UNSORTED":
        if sort_by == "ID":
            _nodes.sort(key=lambda x: x["identifier"])
        if sort_by == "DATE":
            _nodes.sort(
                key=lambda x: x.get("synchronization", {}).get("lastHarvested", "")
            )
    if len(tests) > 0:
        cn.testNodeConnectivity(tests,  solr_url=ctx.obj["solr_url"], timeout=timeout)
        '''
        targets = []
        for n in cn.nodes():
            targets.append(
                cn.mnStatus(
                    n["identifier"], solr_url=ctx.obj["solr_url"], timeout=timeout
                )
            )
        result = mnstatus.testNodeConnectivity(targets, tests=tests)
        for node_id in result:
            _L.warning("NODE_ID = %s", node_id)
            _L.warning("INFO = %s", str(result[node_id]))
            res = cn.setStatusInfo(node_id, result[node_id])
            _L.warning("SET = %s", res)
        '''
    if ctx.obj.get("json_format", False):
        if show_full:
            print(mnstatus.jsonDumps(cn.nodes()))
        else:
            res = []
            for n in cn.nodes():
                res.append(
                    {
                        "identifier": n["identifier"],
                        "baseURL": n["baseURL"],
                        "name": n["name"],
                        "@state": n["@state"],
                        "@type": n["@type"],
                        "lastHarvested": n.get("synchronization", {}).get(
                            "lastHarvested", ""
                        ),
                    }
                )
            print(mnstatus.jsonDumps(res))
        return 0
    for n in cn.nodes():
        lh = n.get("synchronization", {}).get("lastHarvested", "")
        print(
            f"{n['identifier']:25} {n['@type']} {stateInt(n['@state'])} {n['baseURL']:55} {lh}"
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
    cn.testNodeConnectivity(tests, solr_url=ctx.obj["solr_url"], timeout=timeout, node_ids_to_test=[node_id,])
    mn = cn.node(node_id)
    print(mnstatus.jsonDumps(mn["status"]))
    '''
    mn = cn.mnStatus(node_id, solr_url=ctx.obj["solr_url"], timeout=timeout)
    if mn is None:
        _L.info("Using CN URL of %s", ctx.obj["cnode_url"])
        _L.error("Unable to locate a node identified by: %s", node_id)
        return 1
    _L.info("testing...")
    result = mnstatus.testNodeConnectivity(
        [
            mn,
        ],
        tests=tests,
    )
    _L.info("done...")
    print(mnstatus.jsonDumps(result))
    '''
    return 0


if __name__ == "__main__":
    sys.exit(main())
