package higgla.client;

import juglr.Box;
import juglr.BoxParser;
import juglr.JSonBoxParser;
import juglr.JSonBoxReader;
import juglr.net.HTTP;
import static juglr.net.HTTP.*;
import juglr.net.HTTPRequestWriter;
import juglr.net.HTTPResponseReader;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

/**
 *
 */
public class Session {

    public static final String USER_AGENT = "Higgla/0.0.1";

    private InetAddress host;
    private String base;
    private int port;
    private InetSocketAddress socketAddress;

    public Session(String host, int port, String base)
                                                   throws UnknownHostException {
        this.host = InetAddress.getByName(host);
        this.port = port;
        this.base = base;
        this.socketAddress = new InetSocketAddress(host, port);
    }

    public Box newDocument(String id, String... indexFields) {
        Box box = Box.newMap();
        box.put("__base__", base);
        box.put("__id__", id);

        if (indexFields.length > 0) {
            Box index = Box.newList();
            box.put("__index__", index);
            for (String field : indexFields) {
                index.add(field);
            }
        }

        return box;
    }

    public Box newDocument(String id, Iterable<String> indexFields) {
        Box box = newDocument(id);
        Box index = null;
        for (String field : indexFields) {
            if (index == null) {
                index = Box.newList();
                box.put("__index__", index);
            }
            index.add(field);
        }
        return box;
    }

    public Query prepareQuery() {
        return new Query(base);
    }

    public List<Box> sendQuery(Query q) throws IOException, HigglaException {
        Box rawQuery = q.getRawQuery();
        Box resp = send(HTTP.Method.POST, "/actor/query/", rawQuery);
        return resp.getList("__results__");
    }

    public Box store(Box... boxes) throws IOException, HigglaException {
        Box envelope = Box.newMap();
        Box list = Box.newList();
        for (Box box : boxes) {
            list.add(box);
        }
        envelope.put("__store__", list);
        return send(HTTP.Method.POST, "/actor/store/", envelope);
    }

    public Box store(Iterable<Box> boxes) throws IOException, HigglaException {
        Box envelope = Box.newMap();
        Box list = Box.newList();
        for (Box box : boxes) {
            list.add(box);
        }
        envelope.put("__store__", list);
        return send(HTTP.Method.POST, "/actor/store/", envelope);
    }

    public Box send(HTTP.Method method, String address, Box box)
                                           throws IOException, HigglaException {
        return send(method, address, new JSonBoxReader(box));
    }

    public Box send(HTTP.Method method, String address, Reader msg)
                                           throws IOException, HigglaException {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        SocketChannel channel = SocketChannel.open(socketAddress);

        // Send request
        HTTPRequestWriter w = new HTTPRequestWriter(channel, buf);
        w.writeMethod(method);
        w.writeUri(address);
        w.writeVersion(HTTP.Version.ONE_ZERO);
        w.writeHeader("User-Agent", USER_AGENT);
        w.startBody();

        char[] cbuf = new char[1024];
        int len;
        while ((len = msg.read(cbuf)) != -1) {
            w.writeBody(CharBuffer.wrap(cbuf, 0, len));
        }
        w.flush();

        // Read response
        buf.clear();
        HTTPResponseReader r = new HTTPResponseReader(channel, buf);
        Version v = r.readVersion();
        if (v == Version.ERROR || v == Version.UNKNOWN) {
            throw new HigglaException("Bad protocol version version");
        }
        int status = r.readStatus().httpOrdinal();
        if (status < 200 || status >= 300) {
            channel.close();
            throw new HigglaException("Bad response code " + status);
        }

        byte[] bbuf = new byte[1024];
        while ((len = r.readHeaderField(bbuf)) > 0) {
            // Ignore headers
        }

        BoxParser parser = new JSonBoxParser();
        return parser.parse(new InputStreamReader(r.streamBody()));
    }
}
