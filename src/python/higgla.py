"""
Client library for interacting with a Higgla server over HTTP/JSON
"""

try:
    import json
except ImportError:
    import simplejson as json

import socket

class Session:
    def __init__ (self, host, port, base):
        self._host = host
        self._port = port
        self._base = base
        self._http = HTTPTransactionFactory(
                           hostname=host, port=port, body_parser=json.load)

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
        conn = self._http.create()
        conn.send_request(method, url, {} ,msg)
        headers, body = conn.read_response()
        return body
        

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
        Error.__init__(self, msg)

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

    print "LOOKUP mydoc1 ND mydoc2 RESULTS"
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
