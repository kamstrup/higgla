"""
Client library for interacting with a Higgla server over HTTP/JSON
"""
try:
    import json
except ImportError:
    import simplejson as json

import higgla.net

class Session:
    """
    A connection to a Higgla server configured to talk to a given base.
    """
    def __init__ (self, base, host="localhost", port=4567):
        """
        Create a new session

        :param base: Name of the base this session should query and store boxes
            in
        :param host: The host name of the server running the Higgla service.
            The default host is `localhost`
        :param port: The port on which the remote Higgla service is available.
            The default port for the Higgla server is 4567.
        """
        self._host = host
        self._port = port
        self._base = "/" + base
        self._http = higgla.net.HTTPTransactionFactory(
                           hostname=host, port=port, body_parser=json.load)

    def prepare_box(self, id, revision, *index, **kwargs):
        """
        Prepare a new empty box for storage in the base. The return value
        is a :const:`dict` type

        :param id: The unique string id for the box
        :param revision: The revision number last seen for this box. Use 0 for
            creating a new box. This version number `must` match the version
            number that is current in the Higgla base; otherwise :meth:`store`
            will raise a :class:VersionConflict when you try and store the box
        """
        if not isinstance(id, (str,unicode)) :
            raise TypeError("Box id must be string or unicode, found %s" % type(id))
        if not isinstance(revision, (int,long)):
            raise TypeError("Box revision must be int or long, found %s" % type(revision))
        box = {}
        box["_id"] = id
        box["_rev"] = revision
        box["_index"] = index
        box.update(kwargs)
        return box

    def prepare_query(self, **kwargs):
        q = Query(self._base)

        if kwargs:
            q.add_template(kwargs)
        return q

    def send_query(self, **queries):
        q = {}
        for name, query in queries.iteritems() : q[name] = query._q
        return self.send("GET", self._base, q)

    def get(self, *ids):
        if not isinstance(ids, (list,tuple)):
            raise TypeError(
                       "Ids must be a list or tuple, found %s" % type(ids))
        return self.send("GET", self._base, ids)

    def store(self, *boxes):
        msg = {}
        for box in boxes:
            msg[box["_id"]] = box
        return self.send("POST", self._base, msg)

    def send(self, method, url, msg):
        """
        Expert: Send a free form message to the Higgla server

        :param method:
        :param uri:
        :param msg:
        :returns:
        """
        conn = self._http.create()
        conn.send_request(method, url, {} ,msg)
        headers, body = conn.read_response()
        self._check_error(body)
        return body

    def _check_error(self, body):
        if isinstance(body,dict) and body.has_key("error"):
            error = body["error"]
            if error == "conflict":
                raise VersionConflict(body["__id__"], box["__rev__"])
            raise HigglaException(error)
        

class Query:
    def __init__ (self, base):
        self._q = {}
        self._q["_templates"] = []

    def add_template(self, template):
        if not isinstance(template, dict):
            raise TypeError(
                       "Query template must be a dict, found %s" % type(dict))
        self._q["_templates"].append(template)
        return self

    def set_count(self, count):
        self._q["_count"] = count
        return self

    def set_offset(self, offset):
        self._q["_offset"] = offset
        return self

class HigglaException (Exception):
    """
    Represents a server side error
    """
    def __init__ (self, msg):
        Exception.__init__(self, msg)

class VersionConflict (HigglaException):
    def __init__ (self, box_id, expected_revision):
        HigglaException.__init__(
         self, "Expected revision %s of box '%s'" % (expected_revision, box_id))
        self._box_id = box_id
        self._expected_revision = expected_revision

    def get_box_id(self) :
        return self._box_id
    id = property(get_box_id, doc="")

    def get_expected_revision(self):
        return self._expected_revision
    expected_revision = property(get_expected_revision)

