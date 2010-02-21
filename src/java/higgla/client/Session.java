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

    public static final String USER_AGENT = "Higgla/0.0.2";

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
     * @param revision the revision number last seen for this box. Set to 0
     *                 for new boxes
     * @param indexFields A list of fields to create full text indexes for.
     *                    It is not an error if these fields are not present in
     *                    the document when stored
     * @return A box of {@code MAP} type that you can add fields to
     */
    public Box prepareBox(String id, long revision, String... indexFields) {
        Box box = Box.newMap();
        box.put("_id", id);
        box.put("_rev", revision);

        if (indexFields.length > 0) {
            Box index = Box.newList();
            box.put("_index", index);
            for (String field : indexFields) {
                index.add(field);
            }
        }

        return box;
    }

    /**
     * Prepare a box for storage in Higgla. When you have build the
     * box you commit it to the storage by calling {@link #store}.
     * @param id The id the document should be stored under
     * @param revision the revision number last seen for this box. Set to 0
     *                 for new boxes
     * @param indexFields A list of fields to create full text indexes for.
     *                    It is not an error if these fields are not present in
     *                    the document when stored
     * @return A box of {@code MAP} type that you can add fields to
     */
    public Box prepareBox(
                       String id, long revision, Iterable<String> indexFields) {
        Box box = prepareBox(id, revision);
        Box index = null;
        for (String field : indexFields) {
            if (index == null) {
                index = Box.newList();
                box.put("_index", index);
            }
            index.add(field);
        }
        return box;
    }

    /**
     * Prepare a named query against the base this session is constructed for.
     * To submit the query use {@link #sendQuery}
     * @param queryName a name used to identify the query. This is useful
     *                  if you submit queries in batches
     * @return a query you should add templates to, and later submit to
     *         {@link #sendQuery}
     */
    public Query prepareQuery(String queryName) {
        return new Query(queryName);
    }

    /**
     * Send a batch of prepared queries to the storage, retrieving all matching
     * boxes (considering also each query's offset and count).
     * @param queries a variable number of named queries to send
     * @return a map mapping query names to result sets. Each result set is a
     *         map with the keys {@code _count}, {@code total},
     *         and {@code data}, that last one containing a list of results
     * @throws IOException if there is an error sending the query
     * @throws HigglaException if the server returns an error message
     */
    public Box sendQuery(Query... queries) throws IOException, HigglaException {
        Box envelope = Box.newMap();
        for (Query q : queries) {
            envelope.put(q.getName(), q.getRawQuery());
        }
        return send(HTTP.Method.GET, "/"+base, envelope);
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
        for (Box box : boxes) {
            envelope.put(box.get("_id").getString(), box);
        }
        return send(HTTP.Method.POST, "/"+base, envelope);
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
        for (Box box : boxes) {
            envelope.put(box.get("_id").getString(), box);
        }
        return send(HTTP.Method.POST, "/"+base, envelope);
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
            throw new HigglaException(box.get("error").toString());
        }
    }
}
