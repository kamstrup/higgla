"""
Client library for interacting with a Higgla server over HTTP/JSON
"""

try:
    import json
except ImportError:
    import simplejson as json

import httplib

class Session:
    def __init__ (self, host, port, base):
        self._host = host
        self._port = port
        self._base = base

    def prepare_box(self, id, *index, **kwargs):
        box = {}
        box["__base__"] = self._base
        box["__id__"] = id
        box["__index__"] = index
        box.update(kwargs)
        return box

    def prepare_query(self, **kwargs):
        q = Query(self._base)

        if kwargs:
            q.add_template(kwargs)
        return q

    def send_query(self, query):
        return self.send("POST", "/actor/query/", query._q)

    def get(self, ids):
        if not isinstance(ids, list):
            raise TypeError(
                       "Ids must be a list, found %s" % type(dict))
        msg = {"__ids__" : ids, "__base__" : self._base}
        return self.send("POST", "/actor/get/", msg)

    def store(self, boxes):
        if not isinstance(boxes, list):
            raise TypeError(
                       "Expected list, found %s" % type(dict))
        msg = {"__store__" : boxes}
        return self.send("POST", "/actor/store/", msg)

    def send(self, method, url, msg):
        """
        Expert: Send a free form message to the Higgla server

        :param method:
        :param uri:
        :param msg:
        :returns:
        """
        if not isinstance(msg, (str,unicode)):
            msg = json.dumps(msg)
        conn = httplib.HTTPConnection(self._host, self._port)
        conn.request(method, url, msg)
        resp = conn.getresponse()
        return json.loads(resp.read())
        

class Query:
    def __init__ (self, base):
        self._q = {}
        self._q["__base__"] = base
        self._q["__query__"] = []

    def add_template(self, template):
        if not isinstance(template, dict):
            raise TypeError(
                       "Query template must be a dict, found %s" % type(dict))
        self._q["__query__"].append(template)

if __name__ == "__main__":
    session = Session("localhost", 4567, "mybase")

    print "LOOKUP mydoc1 and mydoc2 RESULTS"
    print str(session.get(["mydoc1", "mydoc2"]))
    print ""

    print "QUERY RESULTS"
    q = session.prepare_query(firstname="john")
    print str(session.send_query(q))
    print ""

    print "STORE RESULTS"
    box = session.prepare_box("mke", "firstname",
                              firstname="Mikkel", lastname="kamstrup")
    print str(session.store([box]))
    print ""