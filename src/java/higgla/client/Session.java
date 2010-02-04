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

    /**
     * Prepare a document for storage in Higgla. When you have build the
     * document you commit it to the storage by calling
     * {@link #store}.
     * @param id The id the document should be stored under
     * @param indexFields A list of fields to create full text indexes for.
     *                    It is not an error if these fields are not present in
     *                    the document when stored
     * @return A box of {@code MAP} type that you can add fields to
     */
    public Box newDocument(String id, long revision, String... indexFields) {
        Box box = Box.newMap();
        box.put("__id__", id);
        box.put("__rev__", revision);

        if (indexFields.length > 0) {
            Box index = Box.newList();
            box.put("__index__", index);
            for (String field : indexFields) {
                index.add(field);
            }
        }

        return box;
    }

    /**
     * Prepare a document for storage in Higgla. When you have build the
     * document you commit it to the storage by calling
     * {@link #store}.
     * @param id The id the document should be stored under
     * @param indexFields A list of fields to create full text indexes for.
     *                    It is not an error if these fields are not present in
     *                    the document when stored
     * @return A box of {@code MAP} type that you can add fields to
     */
    public Box newDocument(
                       String id, long revision, Iterable<String> indexFields) {
        Box box = newDocument(id, revision);
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

    /**
     * Prepare a query against the base this session is constructed for.
     * To submit the query use {@link #sendQuery}
     * @return a query you should add templates to, and later submit to
     *         {@link #sendQuery}
     */
    public Query prepareQuery() {
        return new Query(base);
    }

    /**
     * Send a prepared query to the storage, retrieving all matching boxes
     * @param q the query to submit
     * @return a list of all matching boxes
     * @throws IOException if there is an error sending the query
     * @throws HigglaException if the server returns an error message
     */
    public List<Box> sendQuery(Query q) throws IOException, HigglaException {
        Box rawQuery = q.getRawQuery();
        Box resp = send(HTTP.Method.POST, "/actor/query/", rawQuery);
        return resp.getList("__results__");
    }

    /**
     * Store one or more boxes in the base this session was created for.
     * @param boxes the boxes to store
     * @return A box of {@code MAP} type that contains {@code (id,status)}
     *         pairs for each box in {@code boxes}. If the status is anything
     *         other than {@code "ok"} the box with the given id has not been
     *         stored
     * @throws IOException if there is an error sending the query
     * @throws HigglaException if the server returns an error message
     */
    public Box store(Box... boxes) throws IOException, HigglaException {
        Box envelope = Box.newMap();
        Box list = Box.newList();
        for (Box box : boxes) {
            list.add(box);
        }
        envelope.put("__store__", list);
        envelope.put("__base__", base);
        return send(HTTP.Method.POST, "/actor/store/", envelope);
    }

    /**
     * Store one or more boxes in the base this session was created for.
     * @param boxes the boxes to store
     * @return A box of {@code MAP} type that contains {@code (id,status)}
     *         pairs for each box in {@code boxes}. If the status is anything
     *         other than {@code "ok"} the box with the given id has not been
     *         stored
     * @throws IOException if there is an error sending the query
     * @throws HigglaException if the server returns an error message
     */
    public Box store(Iterable<Box> boxes) throws IOException, HigglaException {
        Box envelope = Box.newMap();
        Box list = Box.newList();
        for (Box box : boxes) {
            list.add(box);
        }
        envelope.put("__store__", list);
        return send(HTTP.Method.POST, "/actor/store/", envelope);
    }

    /**
     * Expert: Send a free form {@link Box} to the Higgla server
     * @param method the HTTP method to use
     * @param address the URI part of the HTTP request
     * @param box the box to send
     * @return the response from the Higgla server
     * @throws IOException if there is an error sending the query
     * @throws HigglaException if the server returns an error message
     */
    public Box send(HTTP.Method method, String address, Box box)
                                           throws IOException, HigglaException {
        return send(method, address, new JSonBoxReader(box));
    }

    /**
     * Expert: Send a free form message to the Higgla server
     * @param method the HTTP method to use
     * @param address the URI part of the HTTP request
     * @param msg the data to send. Note that the Higgla server expects JSON
     *            data as the body of all HTTP requests
     * @return the response from the Higgla server
     * @throws IOException if there is an error sending the query
     * @throws HigglaException if the server returns an error message
     */
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
            throw new IOException("Bad protocol version version");
        }
        int status = r.readStatus().httpOrdinal();
        if (status < 200 || status >= 300) {
            channel.close();
            throw new HigglaException("Bad response code " + status);
        }

        byte[] bbuf = new byte[1024];
        while (r.readHeaderField(bbuf) > 0) {
            // Ignore headers
        }

        BoxParser parser = new JSonBoxParser();
        Box box = parser.parse(new InputStreamReader(r.streamBody()));
        checkError(box);
        return box;
    }

    /**
     * Throw a {@link higgla.client.HigglaException} if {@code box} is a
     * {@code MAP} and contains a field named {@code ""error}.
     * @param box the box to check for errors
     * @throws HigglaException if box contains an error field
     */
    private void checkError(Box box) throws HigglaException {
        if (box.getType() == Box.Type.MAP && box.has("error")) {
            throw new HigglaException(box.getString("error"));
        }
    }
}
