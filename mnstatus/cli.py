import sys
import logging
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


def getLogFormatter():
    return logging.Formatter("%(asctime)-8s %(levelname)-6s: %(message)-s", "%H:%M:%S")


def stateInt(state):
    if state.lower() == "up":
        return 1
    return 0

def _filterNodeState(nodes, state):
    if state is None:
        return nodes.copy()
    res = []
    for n in nodes:
        if n['@state'] == state:
            res.append(n.copy())
    return res

def _filterNodeType(nodes, _type):
    L = mnstatus.getLogger()
    if _type is None:
        return nodes.copy()
    res = []
    for n in nodes:
        L.debug(mnstatus.jsonDumps(n))
        if n['@type'] == _type:
            res.append(n.copy())
    return res


@click.group()
@click.option(
    "--verbosity", default="INFO", help="Specify logging level", show_default=True
)
@click.option("-J","--json", "json_format", is_flag=True, help="Output in JSON")
@click.pass_context
def main(ctx, verbosity, json_format):
    logger = mnstatus.getLogger()
    logger.setLevel(LOG_LEVELS.get(verbosity.upper(), logging.INFO))
    c_handler = logging.StreamHandler()
    c_handler.setFormatter(getLogFormatter())
    logger.addHandler(c_handler)
    ctx.ensure_object(dict)
    ctx.obj["json_format"] = json_format
    return 0


@main.command("nids", short_help="List nodes from CN node list")
@click.option("-t", "--n_type", default=None, help="Specify node type, mn or cn")
@click.option("-s", "--state", "n_state", default=None, help="Specify node state, up or down")
@click.option("-F", "--full", "show_full", is_flag=True, help="Show full node records")
@click.argument("base_url", required=False)
@click.pass_context
def listNodes(ctx, n_type, n_state, show_full, base_url=None):
    L = mnstatus.getLogger()
    if not n_type is None:
        n_type = n_type.lower()
    if n_type not in [None, 'cn', 'mn']:
        L.error("Expecting 'mn' or 'cn' for the node type")
        return 1
    if not n_state is None:
        state = n_state.lower()
    if state not in [None, 'up', 'down']:
        L.error("Expecting node state to be 'up' or 'down'")
        return 1
    if base_url is None:
        base_url = "https://cn.dataone.org/cn"
    cn = mnstatus.NodeList(base_url=base_url)
    #nids = cn.node_ids()
    #nids.sort()
    _nodes = _filterNodeState(cn.nodes(), n_state)
    _nodes = _filterNodeType(_nodes, n_type)
    if ctx.obj.get("json_format"):
        if show_full:
            print(mnstatus.jsonDumps(_nodes))
        else:
            res = []
            for n in _nodes:
                res.append({
                    "identifier":n["identifier"],
                    "baseURL": n["baseURL"],
                    "name": n["name"],
                    "@state": n["@state"],
                    "@type": n["@type"],
                    "lastHarvested": n.get("synchronization",{}).get("lastHarvested","")
                })
            print(mnstatus.jsonDumps(res))
        return 0
    for n in _nodes:
        lh = n.get("synchronization", {}).get("lastHarvested", "")
        print(f"{n['identifier']:25} {n['@type']} {stateInt(n['@state'])} {lh} {n['baseURL']}")
    return 0

@main.command("node", short_help="Check node status")
@click.argument("base_url")
@click.pass_context
def checkNode(ctx, base_url):
    L = mnstatus.getLogger()
    mn = mnstatus.MNStatus(base_url)
    L.info("Ping...")
    result = {"ping": mn.pingStatus()}
    L.info("MN Counts ...")
    result["mn_counts"] = mn.objectInfoFromMN()
    print(mnstatus.jsonDumps(result))


if __name__ == "__main__":
    sys.exit(main())
