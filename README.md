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

Counts and times are based on the DataONE listObjects response, which only includes 
`dateModified`. Hence the entries in the status results are the earliest and latest dates 
that system metadata was modified.

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
entry contains information similar to the output above for a single node.

A summary can be generated using `jq` to extract values. For example with columns
`node_id, ping_status, mn_count, cn_count, index_count`:

```bash
curl -s 'https://raw.githubusercontent.com/datadavev/mnstatus/main/data/node_status.json' | \ 
jq -r '.[] | '\
'[.identifier, .status.ping.status, .status.mn.count, .status.cn.count, .status.index.count ]'\
'| @csv'

"urn:node:KNB",200,177495,44715,28954
"urn:node:ESA",200,253,253,157
"urn:node:SANPARKS",0,,6418,3725
"urn:node:LTER",200,440686,454318,404397
"urn:node:CDL",200,167789,242115,158007
"urn:node:PISCO",200,185241,185214,176564
"urn:node:ONEShare",0,,1993,474
"urn:node:mnORC1",200,44880,0,0
"urn:node:mnUNM1",200,25820,0,0
"urn:node:mnUCSB1",200,37988,689,603
"urn:node:TFRI",0,,8555,5996
"urn:node:SEAD",0,,113,110
"urn:node:GOA",200,3289,3208,1686
"urn:node:LTER_EUROPE",404,,343,343
"urn:node:EDACGSTORE",200,1075,1075,1067
"urn:node:IOE",0,,279,278
"urn:node:US_MPC",200,,1032,1032
"urn:node:IARC",0,,0,1842
"urn:node:NMEPSCOR",200,9304,3685,3658
"urn:node:TERN",200,14727,14632,14631
"urn:node:NKN",200,4670,48,46
"urn:node:USGS_SDC",503,,40185,21984
"urn:node:NRDC",0,,6673,6672
"urn:node:NCEI",200,51001,50972,50967
"urn:node:NEON",200,23836,24221,21692
"urn:node:TDAR",403,,76784,69052
"urn:node:ARCTIC",200,818542,818634,780065
"urn:node:BCODMO",200,35,35,35
"urn:node:GRIIDC",200,8597,8581,8581
"urn:node:R2R",200,1826,1787,1787
"urn:node:EDI",200,6702,6691,6202
"urn:node:UIC",200,11823,0,0
"urn:node:RW",200,2679,2678,2652
"urn:node:FEMC",200,7884,6805,6585
"urn:node:PANGAEA",200,,507566,507546
"urn:node:ESS_DIVE",200,16550,16592,9400
"urn:node:CAS_CERN",200,155,144,143
"urn:node:FIGSHARE_CARY",200,5,5,5
"urn:node:IEDA_EARTHCHEM",200,890,890,888
"urn:node:IEDA_USAP",200,735,695,695
"urn:node:IEDA_MGDL",200,11049,9734,9733
"urn:node:METAGRIL",200,279,277,266
"urn:node:ARM",200,11744,11439,11439
"urn:node:CA_OPC",200,3892,3699,457
```

