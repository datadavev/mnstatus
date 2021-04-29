import logging
import logging.handlers
import time
import datetime
import json
import asyncio
import concurrent.futures
import socket
import urllib.parse
import urllib3
import requests
import dateparser
import xmltodict


MAXIMUM_CONCURRENCY = 12
"""Maximum number of concurrent tasks. 
"""

MAXIMUM_INDEX_TASKS = 5
MAXIMUM_CN_TASKS = 3


HTTP_TIMEOUT = 20.0
"""HTTP request timeout in seconds
"""

JSON_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
"""datetime format string for generating JSON content
"""

DATAONE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

DATAONE_OBJECT_LIST = "http://ns.dataone.org/service/types/v1:objectList"

SOLR_URL = "query/solr/"

SOLR_RESERVED_CHAR_LIST = [
    "+",
    "-",
    "&",
    "|",
    "!",
    "(",
    ")",
    "{",
    "}",
    "[",
    "]",
    "^",
    '"',
    "~",
    "*",
    "?",
    ":",
]


def getLogger():
    return logging.getLogger("mnstatus")


def escapeSolrQueryTerm(term):
    term = term.replace("\\", "\\\\")
    for c in SOLR_RESERVED_CHAR_LIST:
        term = term.replace(c, "\{}".format(c))
    return term


def dtToDataONETime(dt):
    dt1 = dt.astimezone(datetime.timezone.utc)
    return dt1.strftime(DATAONE_TIME_FORMAT)


def dtnow():
    """Get datetime for now in UTC timezone."""
    return datetime.datetime.now(datetime.timezone.utc)


def datetimeFromString(v):
    return dateparser.parse(
        v, settings={"TIMEZONE": "+0000", "RETURN_AS_TIMEZONE_AWARE": True}
    )


def datetimeToJsonStr(dt):
    """Convert datetime to a JSON compatible string"""
    if dt is None:
        return None
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        # Naive timestamp, convention is this must be UTC
        return f"{dt.strftime(JSON_TIME_FORMAT)}Z"
    return dt.strftime(JSON_TIME_FORMAT)


def _jsonConverter(o):
    if isinstance(o, datetime.datetime):
        return datetimeToJsonStr(o)
    return o.__str__()


def jsonDumps(obj):
    """Dump object as JSON, handling date conversion"""
    return json.dumps(obj, indent=2, default=_jsonConverter, sort_keys=True)


class ObjectList:
    def __init__(
        self,
        lo_url,
        offset=0,
        max_entries=-1,
        from_date=None,
        to_date=None,
        page_size=1000,
    ):
        self._url = lo_url
        self._start_offset = offset
        self._max_entries = max_entries
        self._page_size = page_size
        if self._max_entries > 0 and self._max_entries < self._page_size:
            self._page_size = self._max_entries
        self._from_date = from_date
        self._to_date = to_date
        self._cpage = None  # current page buffer
        self._coffset = self._start_offset  # offset in MN "list"
        self._page_offset = 0  # offset within a page
        self._total_records = 0  # total records on MN
        self._started = False
        self._session = requests.Session()

    def __iter__(self):
        return self

    def __len__(self):
        return self._total_records

    def __next__(self):
        _L = getLogger()
        if not self._started:
            _L.debug("Starting object iteration")
            self._getPage()
            self._started = True
        if self._cpage is None:
            _L.debug("Out of pages")
            raise StopIteration
        if self._max_entries > 0 and self._coffset >= self._max_entries:
            _L.debug("Exceeding max entries, stopping")
            raise StopIteration
        if self._page_offset >= len(self._cpage):
            self._getPage()
        try:
            entry = self._cpage[self._page_offset]
            self._page_offset += 1
            self._coffset += 1
            return entry
        except KeyError as e:
            _L.warning("Stop from %s", e)
        except TypeError as e:
            _L.warning("Stop from %s", e)
        except ValueError as e:
            _L.warning("Stop from %s", e)
        raise StopIteration

    def _getPage(self):
        _L = getLogger()
        params = {
            "start": self._coffset,
            "count": self._page_size,
        }
        if not self._from_date is None:
            params["fromDate"] = dtToDataONETime(self._from_date)
        if not self._to_date is None:
            params["toDate"] = dtToDataONETime(self._to_date)
        _L.debug("request params: %s", params)
        response = self._session.get(self._url, params=params)
        if response.status_code != 200:
            _L.error("Status: %s. Reason: %s", response.status_code, response.reason)
            return None
        olist = xmltodict.parse(response.text, process_namespaces=True)
        self._total_records = int(olist[DATAONE_OBJECT_LIST]["@total"])
        self._cpage = []
        for entry in olist[DATAONE_OBJECT_LIST].get("objectInfo", []):
            try:
                r = {
                    "identifier": entry["identifier"],
                    "formatId": entry["formatId"],
                    "checksum_algorithm": entry["checksum"]["@algorithm"],
                    "checksum": entry["checksum"]["#text"],
                    "dateSysMetadataModified": datetimeFromString(
                        entry["dateSysMetadataModified"]
                    ),
                    "size": int(entry["size"]),
                }
            except Exception as e:
                _L.error("ERROR converting record: %s", e)
            self._cpage.append(r)
        self._page_offset = 0


class MNStatus(object):
    def __init__(
        self, node_id, base_url, cn_url, solr_url, version=2, timeout=HTTP_TIMEOUT
    ):
        self.node_id = node_id
        self.base_url = base_url
        self.cn_url = cn_url
        self.solr_url = solr_url
        if not self.base_url[-1] == "/":
            self.base_url = self.base_url + "/"
        if not self.solr_url.startswith("http"):
            self.solr_url = urllib.parse.urljoin(self.cn_url, "v2/" + solr_url)
        self.version = "v2/"
        if version < 2:
            self.version = "v1/"
        self.timeout = timeout
        self._session = None

    def _doget(
        self, url, params=None, verify=True, call_back=None, timeout=HTTP_TIMEOUT
    ):
        _L = getLogger()
        t_start = dtnow()
        try:
            response = requests.get(url, params=params, timeout=timeout, verify=verify)
            msg = ""
            if response.status_code != 200:
                msg = response.reason
            elif not verify:
                msg = "No certificate validation"
            return (response.status_code, t_start, msg, response)
        except requests.exceptions.SSLError as e:
            if verify:
                _L.warning("Retrying %s with no certificate validation", url)
                return self._doget(
                    url, params=params, verify=False, call_back=call_back
                )
            else:
                _L.warning(e)
                return (-1, t_start, str(e), None)
        except Exception as e:
            _L.warning(e)
            return (0, t_start, str(e), None)

    def _objectModifiedDates(self, t0, t1, list_objects_url, xparams={}, task_name=""):
        """Return a list of [dateModified, PID]"""
        _L = getLogger()
        params = xparams
        if t0 is not None:
            params["fromDate"] = dtToDataONETime(t0)
        if t1 is not None:
            params["toDate"] = dtToDataONETime(t1)
        result = {"total": 0, "count": 0, "object": [], "status": 0}
        response = None
        try:
            response = requests.get(
                list_objects_url, params=params, timeout=self.timeout
            )
        except requests.exceptions.SSLError as e:
            result["status"] = -1
            return result
        except urllib3.exceptions.MaxRetryError as e:
            result["status"] = -2
            return result
        except requests.exceptions.ReadTimeout as e:
            result["status"] = -3
            return result
        except socket.timeout as e:
            result["status"] = -4
            return result
        result["status"] = response.status_code
        try:
            olist = xmltodict.parse(response.text, process_namespaces=True)
        except Exception as e:
            _L.error("Failed to parse XML response from URL: %s", response.url)
            _L.error(e)
            result["status"] = -3
            return result
        if response.status_code != 200:
            return result
        result["total"] = int(olist[DATAONE_OBJECT_LIST]["@total"])
        result["count"] = int(olist[DATAONE_OBJECT_LIST]["@count"])
        _L.debug("%s %s count = %s", task_name, self.node_id, result["count"])
        result["objects"] = []
        if result["count"] == 1:
            objects = []
            entry = olist[DATAONE_OBJECT_LIST]["objectInfo"]
            obj = [
                dateparser.parse(entry["dateSysMetadataModified"]),
                entry["identifier"],
            ]
            objects.append(obj)
            result["objects"] = objects
        elif result["count"] > 1:
            objects = []
            for entry in olist[DATAONE_OBJECT_LIST]["objectInfo"]:
                obj = [
                    dateparser.parse(entry["dateSysMetadataModified"]),
                    entry["identifier"],
                ]
                objects.append(obj)
            result["objects"] = sorted(objects, key=lambda x: x[0])
        return result

    def grokMNDates(self, list_objects_url, xparams={}, task_name=""):
        """
        This is a little insane. Metacat does not sort listObjects by dateModified. This makes
        determining the range of modified dates on an MN a bit of an exercise.
        """
        # TODO: need to also check that count == total, if not then make a smaller page
        # oldest
        _L = getLogger()
        _L.info("%s %s grok date range", task_name, self.node_id)
        y_max = dtnow().year
        y_start = datetimeFromString(f"2012-01-01")
        d_delta = 180
        _found = False
        results = {}
        d_offs = 0
        t1 = y_start + datetime.timedelta(days=d_offs)
        while not _found and t1.year <= y_max:
            _L.debug("%s %s oldest, t1 = %s", task_name, self.node_id, t1)
            olist = self._objectModifiedDates(
                None, t1, list_objects_url, xparams=xparams, task_name=task_name
            )
            if olist["total"] > 0:
                _found = True
                results["earliest"] = olist["objects"][0][0]
                results["earliest_pid"] = olist["objects"][0][1]
            d_offs = d_offs + d_delta
            t1 = y_start + datetime.timedelta(days=d_offs)
        # newest
        y_min = 2012
        t1 = dtnow()
        d_delta = 2.0
        d_offs = 2
        d_inc = d_offs
        t0 = t1 - datetime.timedelta(days=d_offs)
        _found = False
        while not _found and t0.year >= y_min:
            _L.debug("%s %s newest, t0 = %s, t1 = %s", task_name, self.node_id, t0, t1)
            olist = self._objectModifiedDates(
                t0, t1, list_objects_url, xparams=xparams, task_name=task_name
            )
            if olist["total"] > 0:
                _found = True
                results["latest"] = olist["objects"][-1][0]
                results["latest_pid"] = olist["objects"][-1][1]
            else:
                d_inc = int(d_inc * d_delta)
                if d_inc > 365:
                    d_inc = 365
                d_offs = d_offs + d_inc
                t0 = t1 - datetime.timedelta(days=d_offs)
        return results

    def pingStatus(self):
        url = urllib.parse.urljoin(self.base_url, self.version + "monitor/ping")
        t0 = time.time()
        result = self._doget(url, timeout=5)
        t1 = time.time()
        res = {
            "method": "ping",
            "url": url,
            "elapsed": t1 - t0,
            "status": result[0],
            "tstamp": result[1],
            "message": result[2],
        }
        return res

    def objectInfoFromMN(self):
        _L = getLogger()
        url = urllib.parse.urljoin(self.base_url, self.version + "object")
        params = {"start": 0, "count": 2}
        result = {
            "method": "mn.listObjects",
            "elapsed": 0,
            "url": url,
            "status": 0,
            "message": "",
            "tstamp": dtnow(),
            "count": None,
            "earliest_pid": None,
            "earliest": None,
            "latest_pid": None,
            "latest": None,
        }
        _L.info("get total %s", url)
        t0 = time.time()
        res = self._doget(url, params=params, timeout=self.timeout)
        if res[0] != 200:
            result["status"] = res[0]
            result["tstamp"] = res[1]
            result["message"] = res[2]
            result["elapsed"] = time.time() - t0
            return result
        result["status"] = res[0]
        counts = xmltodict.parse(res[3].text, process_namespaces=True)
        result["count"] = int(counts[DATAONE_OBJECT_LIST]["@total"])
        list_objects_url = urllib.parse.urljoin(self.base_url, self.version + "object")
        date_range = self.grokMNDates(list_objects_url, task_name="mn")
        result.update(date_range)
        t1 = time.time()
        result["elapsed"] = t1 - t0
        return result

    def objectInfoFromCN(self):
        _L = getLogger()
        list_objects_url = urllib.parse.urljoin(self.cn_url, "v2/object")
        params = {"start": 0, "count": 2, "nodeId": self.node_id}
        result = {
            "method": "cn.listObjects",
            "elapsed": 0,
            "url": list_objects_url,
            "status": 0,
            "message": "",
            "tstamp": dtnow(),
            "count": None,
            "earliest_pid": None,
            "earliest": None,
            "latest_pid": None,
            "latest": None,
        }
        _L.info("cn %s get total %s", self.node_id, list_objects_url)
        t0 = time.time()
        res = self._doget(list_objects_url, params=params, timeout=self.timeout)
        if res[0] != 200:
            result["status"] = res[0]
            result["tstamp"] = res[1]
            result["message"] = res[2]
            result["elapsed"] = time.time() - t0
            return result
        result["status"] = res[0]
        counts = xmltodict.parse(res[3].text, process_namespaces=True)
        result["count"] = int(counts[DATAONE_OBJECT_LIST]["@total"])

        date_range = self.grokMNDates(
            list_objects_url, xparams={"nodeId": self.node_id}, task_name="cn"
        )
        result.update(date_range)
        t1 = time.time()
        result["elapsed"] = t1 - t0
        return result

    def objectInfoFromIndex(self):
        _L = getLogger()
        params = {
            "wt": "json",
            "start": 0,
            "rows": 5,
            "fl": "id,seriesId,formatId,dateModified,dateUploaded",
            "q": f"datasource:{escapeSolrQueryTerm(self.node_id)}",
            "sort": "dateModified asc",
        }
        result = {
            "method": "cn.index",
            "elapsed": 0,
            "url": self.solr_url,
            "status": 0,
            "message": "",
            "tstamp": dtnow(),
            "count": 0,
            "earliest_pid": None,
            "earliest_sid": None,
            "earliest": None,
            "earliest_uploaded": None,
            "latest_pid": None,
            "latest_sid": None,
            "latest": None,
            "latest_uploaded": None,
        }
        session = requests.Session()
        t0 = time.time()
        _L.info("index %s %s", self.node_id, self.solr_url)
        response = session.get(self.solr_url, params=params, timeout=self.timeout)
        try:
            res_1 = json.loads(response.text)
        except json.decoder.JSONDecodeError as e:
            result["status"] = -5
            result["message"] = str(e)
            return result
        t1 = time.time()
        result["elapsed"] = t1 - t0
        result["count"] = int(res_1["response"]["numFound"])
        if result["count"] > 0:
            result["earliest_pid"] = res_1["response"]["docs"][0]["id"]
            result["earliest_sid"] = res_1["response"]["docs"][0].get("series_id", None)
            result["earliest"] = res_1["response"]["docs"][0]["dateModified"]
            result["earliest_uploaded"] = res_1["response"]["docs"][0]["dateUploaded"]
        params["sort"] = "dateModified desc"
        response = session.get(self.solr_url, params=params, timeout=self.timeout)
        try:
            res_2 = json.loads(response.text)
        except json.decoder.JSONDecodeError as e:
            result["status"] = -5
            result["message"] = str(e)
            return result
        t1 = time.time()
        result["elapsed"] = t1 - t0
        result["status"] = response.status_code
        result["message"] = response.reason
        if result["count"] > 0:
            result["latest_pid"] = res_2["response"]["docs"][0]["id"]
            result["latest_sid"] = res_2["response"]["docs"][0].get("series_id", None)
            result["latest"] = res_2["response"]["docs"][0]["dateModified"]
            result["latest_uploaded"] = res_2["response"]["docs"][0]["dateUploaded"]
        return result


def runCheck(node, task):
    """returns (node_id, task, result)"""
    _L = getLogger()
    res = None
    if task == "ping":
        _L.info("Start Ping %s", node.node_id)
        res = node.pingStatus()
    if task == "mn":
        _L.info("Start MN Counts %s", node.node_id)
        res = node.objectInfoFromMN()
    if task == "cn":
        _L.info("Start CN Counts %s", node.node_id)
        res = node.objectInfoFromCN()
    if task == "index":
        _L.info("Start Index Counts %s", node.node_id)
        res = node.objectInfoFromIndex()
    return (node.node_id, task, res)


class NodeList(object):
    def __init__(self, base_url="https://cn.dataone.org/cn"):
        self.base_url = base_url
        if not self.base_url[-1] == "/":
            self.base_url = self.base_url + "/"
        self._nodes = None

    def _ensureNodes(self):
        _L = getLogger()
        if self._nodes is not None:
            return
        _L.info("Loading node list")
        url = urllib.parse.urljoin(self.base_url, "v2/node")
        response = requests.get(url)
        data = xmltodict.parse(response.text, process_namespaces=True)
        self._nodes = data["http://ns.dataone.org/service/types/v2.0:nodeList"][
            "node"
        ].copy()
        return self._nodes

    def nodes(self):
        self._ensureNodes()
        return self._nodes

    def node_ids(self):
        res = []
        for n in self.nodes():
            res.append(n["identifier"])
        return res

    def node(self, node_id):
        """Get node document given node_id"""
        self._ensureNodes()
        for n in self.nodes():
            if n["identifier"] == node_id:
                return n
        return None

    def baseUrl(self, node_id):
        """Lookup baseURL by nodeID"""
        self._ensureNodes()
        n = self.node(node_id)
        if n is None:
            return n
        return n["baseURL"]

    def nodeId(self, base_url):
        """Lookup nodeID by baseURL"""
        self._ensureNodes()
        for n in self.nodes():
            if n["baseURL"] == base_url:
                return n["identifier"]
        return None

    def nodeServiceVersion(self, node_id, service="MNRead"):
        n = self.node(node_id)
        if n is None:
            return None
        _version = 0
        for svc in n["services"].get("service", []):
            if svc["@name"] == service:
                v = svc["@version"]
                if v.lower() == "v1":
                    v = 1
                    if v > _version:
                        _version = v
                elif v.lower() == "v2":
                    v = 2
                    if v > _version:
                        _version = v
        return _version

    def mnStatus(self, node_id, solr_url=SOLR_URL, timeout=HTTP_TIMEOUT):
        """Get a MNStatus instance given node_id"""
        node = self.node(node_id)
        if node is None:
            return None
        version = self.nodeServiceVersion(node_id)
        return MNStatus(
            node_id,
            node["baseURL"],
            self.base_url,
            solr_url,
            version=version,
            timeout=timeout,
        )

    def setStatusInfo(self, node_id, task, info):
        self._ensureNodes()
        for i in range(0, len(self._nodes)):
            if self._nodes[i]["identifier"] == node_id:
                try:
                    self._nodes[i]["status"][task] = info.copy()
                except KeyError:
                    self._nodes[i]["status"] = {}
                    self._nodes[i]["status"][task] = info.copy()
                return True
        return False

    def filterNodeState(self, state):
        if state is None:
            return
        self._ensureNodes()
        res = []
        for n in self._nodes:
            if n["@state"] == state:
                res.append(n.copy())
        self._nodes = res

    def filterNodeType(self, _type):
        if _type is None:
            return
        self._ensureNodes()
        res = []
        for n in self._nodes:
            if n["@type"] == _type:
                res.append(n.copy())
        self._nodes = res

    def testNodeConnectivity(
        self, tests, solr_url=SOLR_URL, timeout=HTTP_TIMEOUT, node_ids_to_test=None
    ):
        async def runChecks():
            _L = getLogger()
            tasks = []
            pending_tasks = []
            active_task_names = []
            active_task_display = []

            def _checkServerLoad(task):
                _mn, _t = task
                # Allow only one cn task at a time
                if _t == "cn":
                    if active_task_names.count("cn") >= MAXIMUM_CN_TASKS:
                        return False
                # Allow only three index tasks at a time
                elif _t == "index":
                    if active_task_names.count("index") >= MAXIMUM_INDEX_TASKS:
                        return False
                active_task_names.append(_t)
                return True

            def _clearServerLoad(task_name):
                _L.debug("task name = %s", task_name)
                try:
                    active_task_names.remove(task_name)
                except ValueError as e:
                    pass

            with concurrent.futures.ProcessPoolExecutor(
                max_workers=MAXIMUM_CONCURRENCY
            ) as executor:
                test_nodes = self.nodes()
                if isinstance(node_ids_to_test, list):
                    test_nodes = []
                    for nid in node_ids_to_test:
                        test_nodes.append(self.node(nid))
                for n in test_nodes:
                    mn = self.mnStatus(n["identifier"], solr_url, timeout)
                    for t in tests:
                        pending_tasks.append((runCheck, (mn, t)))
                futures_complete = False
                total_tasks = len(pending_tasks)
                while len(pending_tasks) > 0 or not futures_complete:
                    for pending_task in pending_tasks:
                        _job, _params = pending_task
                        if _checkServerLoad(_params):
                            future = executor.submit(_job, *_params)
                            active_task_display.append(
                                f"{_params[1]}::{_params[0].node_id}"
                            )
                            tasks.append(future)
                            pending_tasks.remove(pending_task)
                    _L.info(
                        "total, pending, scheduled jobs = %s, %s, %s",
                        total_tasks,
                        len(pending_tasks),
                        len(tasks),
                    )
                    _L.info("Jobs scheduled: %s", str(active_task_display))
                    try:
                        for future in concurrent.futures.as_completed(tasks, timeout=5):
                            result = future.result()
                            _clearServerLoad(result[1])
                            _L.info("%s %s complete", result[1], result[0])
                            self.setStatusInfo(result[0], result[1], result[2])
                            tasks.remove(future)
                            active_task_display.remove(f"{result[1]}::{result[0]}")

                    except concurrent.futures.TimeoutError:
                        _L.debug("No futures to clear")
                    futures_complete = True
                    for future in tasks:
                        if not future.done():
                            futures_complete = False
                            break

        self._ensureNodes()
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(runChecks())
        loop.run_until_complete(future)
