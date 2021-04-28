# `mnstatus`

Command line tool to evaluate connectivity and object count 
consistency for DataONE member nodes.

## Installation

Simplest is to install using [`pipx`](https://github.com/pipxproject/pipx):

```
pipx install git+https://github.com/datadavev/mnstatus
```

A development install can be made using Poetry:

```
mkvirtualenv mnstatus
git clone https://github.com/datadavev/mnstatus.git
cd mnstatus
poetry install
```

## Operation

```
Usage: mnstatus [OPTIONS] COMMAND [ARGS]...

Options:
  --verbosity TEXT      Specify logging level  [default: INFO]
  -J, --json            Output in JSON
  -C, --cnode_url TEXT  Base URL of Coordinating Node
  --solr_url TEXT       Solr URL absolute or relative to CN base URL.
  --help                Show this message and exit.

Commands:
  nids  List nodes from CN node list
  node  Check node status
```

To check a single member node, use the `node` operation:

```
sage: mnstatus node [OPTIONS] NODE_ID

Options:
  -T, --timeout FLOAT  HTTP connection timeout in seconds
  -t, --test TEXT      Tests to run
  --help               Show this message and exit.
```

Four tests are available:

`ping`: Ping the node

`mn`: Get the number of records, the oldest and most recent System Metadata Modified date as reported by the MN

`cn`: Get the number of records, the oldest and most recent System Metadata Modified date as reported by the CN

`index`: Get the number of records, the oldest and most recent System Metadata Modified date and Uploaded date as reported by the CN index.

Example:
```
mnstatus node urn:node:KNB -t ping -t mn -t cn -t index
{
  "cn": {
    "count": 44715,
    "earliest": "2012-06-14T03:48:31+0000",
    "earliest_pid": "doi:10.5063/AA/Cary.12.1",
    "elapsed": 5.471796274185181,
    "latest": "2021-04-27T04:14:56+0000",
    "latest_pid": "resource_map_urn:uuid:9a3fcb59-186d-4d64-b388-58da1331e878",
    "message": "",
    "method": "cn.listObjects",
    "status": 200,
    "tstamp": "2021-04-28T03:16:33+0000",
    "url": "https://cn.dataone.org/cn/v2/object"
  },
  "index": {
    "count": 28954,
    "earliest": "2012-06-14T03:48:31.183Z",
    "earliest_pid": "doi:10.5063/AA/Cary.12.1",
    "earliest_sid": null,
    "earliest_uploaded": "2010-09-30T23:00:00Z",
    "elapsed": 0.054245948791503906,
    "latest": "2021-04-27T04:14:56.603Z",
    "latest_pid": "resource_map_urn:uuid:9a3fcb59-186d-4d64-b388-58da1331e878",
    "latest_sid": null,
    "latest_uploaded": "2021-04-27T04:14:56.419Z",
    "message": "OK",
    "method": "cn.index",
    "status": 200,
    "tstamp": "2021-04-28T03:16:33+0000",
    "url": "https://cn.dataone.org/cn/v2/query/solr/"
  },
  "mn": {
    "count": 177495,
    "earliest": "2012-06-14T03:48:31+0000",
    "earliest_pid": "doi:10.5063/AA/Cary.12.1",
    "elapsed": 1.7448780536651611,
    "latest": "2021-04-27T18:14:29+0000",
    "latest_pid": "resource_map_urn:uuid:e3c7ee14-348e-48ce-81b7-ba700a94f8c8",
    "message": "",
    "method": "mn.listObjects",
    "status": 200,
    "tstamp": "2021-04-28T03:16:33+0000",
    "url": "https://knb.ecoinformatics.org/knb/d1/mn/v2/object"
  },
  "ping": {
    "elapsed": 0.02126455307006836,
    "message": "",
    "method": "ping",
    "status": 200,
    "tstamp": "2021-04-28T03:16:33+0000",
    "url": "https://knb.ecoinformatics.org/knb/d1/mn/v2/monitor/ping"
  }
} 
```

To check a bunch of registered nodes, use the `nids` operation:

```
Usage: mnstatus nids [OPTIONS]

Options:
  -n, --n_type TEXT              Specify node type, mn or cn
  -s, --state TEXT               Specify node state, up or down
  -F, --full                     Show full node records
  -t, --test TEXT                Tests to run
  -T, --timeout FLOAT            HTTP connection timeout in seconds
  --help                         Show this message and exit.
```

For example, check all the member nodes listed as being in the `up` state:
```
mnstatus --json nids -n mn -s up -t ping -t index  -t mn -t cn -F > test.json
```

The resulting JSON file is pretty big, and is a JSON-ification of
the XML node list with a `status` entry added to each node. That
entry contain information similar to the output above for a single node.

