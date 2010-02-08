"""
Client library for interacting with a Higgla server over HTTP/JSON
"""

try:
    import json
except ImportError:
    import simplejson as json

import socket

class Session:
    """
    A connection to a Higgla server configured to talk to a given base.
    """
    def __init__ (self, host, port, base):
        """
        Create a new session

        :param host: The host name of the server running the Higgla service
        :param port: The port on which the remote Higgla service is available.
            The default port for the Higgla server is 4567.
        :param base: Name of the base this session should query and store boxes
           in
        """
        self._host = host
        self._port = port
        self._base = base
        self._http = HTTPTransactionFactory(
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
        box["__id__"] = id
        box["__rev__"] = revision
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
                       "Expected list, found %s" % type(boxes))
        msg = {"__store__" : boxes, "__base__" : self._base}
        return self.send("POST", "/actor/store/", msg)

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
        if body.has_key("error"):
            error = body["error"]
            if error == "conflict":
                raise VersionConflict(body["__id__"], box["__rev__"])
            raise HigglaException(error)
        

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


class SocketFactory:
    def __init__ (self, hostname, port):
        self._hostname = hostname
        self._port = port
        self._socket = None
        self._lastaddr = None

    def connect(self):
        """
        Return a newly created socket.Socket instance. The last known
        working address will be cached in order to not incur the overhead
        of name resolution on each call. If the cached address fails to
        open the host and port will be resolved again.
        """
        # Try and reuse the old address
        s = None
        if self._lastaddr:
            af, socktype, proto, canonname, sa = self._lastaddr
            try:
                s = socket.socket(af, socktype, proto)
            except socket.error, msg:
                s = None
            try:
                s.connect(sa)
            except socket.error, msg:
                s.close()
                s = None
        if s:
            return s

        # The old address did work, try resolving the address again
        for res in socket.getaddrinfo(
              self._hostname, self._port, socket.AF_UNSPEC, socket.SOCK_STREAM):
            af, socktype, proto, canonname, sa = res
            try:
                s = socket.socket(af, socktype, proto)
            except socket.error, msg:
                s = None
                continue
            try:
                s.connect(sa)
            except socket.error, msg:
                s.close()
                s = None
                continue

            # We are good, cache this address
            self._lastaddr = res
            break
        return s

class HTTPProtocolError (Exception):
    def __init__ (self, msg):
        Exception.__init__(self, msg)

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

class HTTPTransaction:

    def __init__ (self, sock, body_parser=None):
        self._socket = sock
        self._file = None
        self._body_parser = body_parser

    def send_request(self, method, url, headers, body=None):
        msg = "%s %s %s\r\n" % (method, url, "HTTP/1.0")
        for k, v in headers.iteritems():
            msg += "%s: %s\r\n" % (k, v)
        msg += "\r\n"
        if body:
            if isinstance(body, (dict,list)):
                body = json.dumps(body)
            else:
                body = str(body)
            msg += body
        self._socket.sendall(msg)

    def read_response(self, bufsize=1024):
        self._file = self._socket.makefile("r", bufsize)
        self._socket.close()
        self._socket = None

        proto, status, reason = self._file.readline().split()

        if not proto in ("HTTP/1.0", "HTTP/1.1"):
            raise HTTPProtocolError("Illegal protocol declaration %s" % proto)

        status = int(status)
        if status < 100 or status > 999:
            raise HTTPProtocolError("Illegal status code %s" % status)

        headers = {}
        while True:
            line = self._file.readline()
            if line == "\r\n" : break
            k, v = line.split(":")
            headers[k] = v.strip()

        # caller will own self._file
        body_stream = self._file
        self._file = None

        if self._body_parser:
            try:
                body = self._body_parser(body_stream)
            finally:
                body_stream.close()
            return headers, body
        else:
            return headers, body_stream
            
    def close(self):
        if self._socket :
            self._socket.close()
            self._socket = None
        if self._file:
            self._file.close()
            self._file = None

class HTTPTransactionFactory:
    def __init__ (self, socket_factory=None,
                  hostname=None, port=None, body_parser=None):
        if socket_factory:
            self._sockets = socket_factory
        elif hostname and port:
            self._sockets = SocketFactory(hostname, port)
        else:
            raise TypeError("Illegal arguments")

        self._body_parser = body_parser

    def create(self):
        s = self._sockets.connect()
        return HTTPTransaction(s, body_parser=self._body_parser)


if __name__ == "__main__":
    session = Session("localhost", 4567, "mybase")

    print "LOOKUP mydoc1 AND mydoc2 RESULTS"
    print str(session.get(["mydoc1", "mydoc2"]))
    print ""

    print "STORE RESULTS"
    box = session.prepare_box("mke", 4, "firstname",
                              firstname="Mikkel", lastname="Kamstrup")
    box["address"] = "57 Mount Pleasant Street"
    try:
        print str(session.store([box]))
    except HigglaException, e:
        print "Error: %s" % e
    print ""

    print "QUERY RESULTS"
    q = session.prepare_query(firstname="mikkel")
    print str(session.send_query(q))
    print ""
