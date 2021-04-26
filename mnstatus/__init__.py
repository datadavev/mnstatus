import logging
import time
import datetime
import json
import urllib.parse
import requests
import dateparser
import xmltodict

HTTP_TIMEOUT = 20.0
"""HTTP request timeout in seconds
"""

JSON_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
"""datetime format string for generating JSON content
"""

DATAONE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

DATAONE_OBJECT_LIST = "http://ns.dataone.org/service/types/v1:objectList"

DAY_INCREMENTS = [8, 16, 32, 64, 128, 256, 512, 1024]


def getLogger():
    return logging.getLogger("mnstatus")


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
    return json.dumps(obj, indent=2, default=_jsonConverter)


class NodeList(object):
    def __init__(self, base_url="https://cn.dataone.org/cn"):
        self.base_url = base_url
        if not self.base_url[-1] == "/":
            self.base_url = self.base_url + "/"
        self._nodes = None

    def nodes(self):
        if self._nodes is not None:
            return self._nodes
        url = urllib.parse.urljoin(self.base_url, "v2/node")
        response = requests.get(url)
        data = xmltodict.parse(response.text, process_namespaces=True)
        self._nodes = data["http://ns.dataone.org/service/types/v2.0:nodeList"][
            "node"
        ].copy()
        return self._nodes

    def node(self, node_id):
        for n in self.nodes():
            if n["identifier"] == node_id:
                return n
        return None

    def node_ids(self):
        res = []
        for n in self.nodes():
            res.append(n["identifier"])
        return res

    def baseUrl(self, node_id):
        n = self.node(node_id)
        if n is None:
            return n
        return n["baseURL"]


class MNStatus(object):
    def __init__(self, base_url, version=2, timeout=HTTP_TIMEOUT):
        self.base_url = base_url
        if not self.base_url[-1] == "/":
            self.base_url = self.base_url + "/"
        self.version = "v2/"
        if version < 2:
            self.version = "v1/"
        self._timeout = timeout
        self._session = None

    def _doget(self, url, params=None, verify=True, call_back=None):
        L = getLogger()
        t_start = dtnow()
        try:
            response = requests.get(
                url, params=params, timeout=self._timeout, verify=verify
            )
            msg = ""
            if response.status_code != 200:
                msg = response.reason
            elif not verify:
                msg = "No certificate validation"
            return (response.status_code, t_start, msg, response)
        except requests.exceptions.SSLError as e:
            if verify:
                L.warning("Retrying %s with no certificate validation", url)
                return self._doget(
                    url, params=params, verify=False, call_back=call_back
                )
            else:
                L.warning(e)
                return (-1, t_start, str(e), None)
        except Exception as e:
            L.warning(e)
            return (0, t_start, str(e), None)

    def _objectModifiedDates(self, t0, t1):
        """Return a list of [dateModified, PID]"""
        params = {}
        if t0 is not None:
            params["fromDate"] = dtToDataONETime(t0)
        if t1 is not None:
            params["toDate"] = dtToDataONETime(t1)
        url = urllib.parse.urljoin(self.base_url, self.version + "object")
        response = requests.get(url, params=params, timeout=self._timeout)
        olist = xmltodict.parse(response.text, process_namespaces=True)
        result = {}
        result["total"] = int(olist[DATAONE_OBJECT_LIST]["@total"])
        result["count"] = int(olist[DATAONE_OBJECT_LIST]["@count"])
        result["objects"] = []
        if result["count"] > 1:
            objects = []
            for entry in olist[DATAONE_OBJECT_LIST]["objectInfo"]:
                obj = [
                    dateparser.parse(entry["dateSysMetadataModified"]),
                    entry["identifier"],
                ]
                objects.append(obj)
            result["objects"] = sorted(objects, key=lambda x: x[0])
        return result

    def grokMNDates(self):
        """
        This is a little insane. Metacat does not sort listObjects by dateModified. This makes
        determining the range of modified dates on an MN a bit of an exercise.
        """
        # TODO: need to also check that count == total, if not then make a smaller page
        L = getLogger()
        # oldest
        y_max = dtnow().year
        y_start = 2012
        d_delta = 180
        _found = False
        results = {}
        d_offs = 0
        while not _found and y_start <= y_max:
            t1 = datetimeFromString(f"{y_start}-01-01") + datetime.timedelta(
                days=d_offs
            )
            L.debug("oldest, t1 = ", t1)
            olist = self._objectModifiedDates(None, t1)
            if olist["total"] > 0:
                _found = True
                results["earliest"] = olist["objects"][0][0]
                results["earliest_pid"] = olist["objects"][0][1]
            d_offs = d_offs + d_delta
        # newest
        y_min = 2012
        t1 = dtnow()
        t0 = t1 - datetime.timedelta(days=2)
        _found = False
        d_offs = 0
        d_delta = 7
        while not _found and t0.year >= y_min:
            L.debug("newest, t0 = %s, t1 = %s", t0, t1)
            olist = self._objectModifiedDates(t0, t1)
            if olist["total"] > 0:
                _found = True
                results["latest"] = olist["objects"][-1][0]
                results["latest_pid"] = olist["objects"][-1][1]
            else:
                t0 = t1 - datetime.timedelta(days=d_offs)
                d_offs = d_offs + d_delta
        return results

    def pingStatus(self):
        url = urllib.parse.urljoin(self.base_url, self.version + "monitor/ping")
        t0 = time.time()
        result = self._doget(url)
        t1 = time.time()
        return {
            "method": "ping",
            "url": url,
            "elapsed": t1 - t0,
            "status": result[0],
            "tstamp": result[1],
            "message": result[2],
        }

    def objectInfoFromMN(self):
        L = getLogger()
        url = urllib.parse.urljoin(self.base_url, self.version + "object")
        params = {"start": 0, "count": 2}
        result = {
            "method": "mn-objects",
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
        L.info("get total %s", url)
        t0 = time.time()
        res = self._doget(url, params=params)
        if res[0] != 200:
            result["status"] = res[0]
            result["tstamp"] = res[1]
            result["message"] = res[2]
            result["elapsed"] = time.time() - t0
            return result
        result["status"] = res[0]
        counts = xmltodict.parse(res[3].text, process_namespaces=True)
        result["count"] = int(counts[DATAONE_OBJECT_LIST]["@total"])
        date_range = self.grokMNDates()
        result.update(date_range)
        t1 = time.time()
        result["elapsed"] = t1 - t0
        return result

    def objectInfoFromCN(self):
        pass

    def objectInfoFromIndex(self):
        pass
