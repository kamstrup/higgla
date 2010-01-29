package higgla.client;

import juglr.Box;
import juglr.JSonBoxReader;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

/**
 *
 */
public class Session {

    public static final String USER_AGENT = "Higgla/0.0.1";

    private String host;
    private String base;
    private int port;

    public Session(String host, int port, String base){
        this.host = host;
        this.port = port;
        this.base = base;
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
        Box resp = send("POST", "/actor/query/", rawQuery);
        return resp.getList("__results__");
    }

    public void store(Box box) throws IOException, HigglaException {
        send("POST", "/actor/store/", box);
    }

    public Box send(String method, String address, Box box)
                                           throws IOException, HigglaException {
        return send(method, address, new JSonBoxReader(box));
    }

    public Box send(String method, String address, Reader msg)
                                           throws IOException, HigglaException {
        Socket socket = new Socket(host, port);
        Writer out = new BufferedWriter(
                            new OutputStreamWriter(socket.getOutputStream()));
        out.append(method).append(" ").append(address).append(" / HTTP/1.0\r\n");
        out.append("User-Agent: ").append(USER_AGENT).append("\r\n");
        out.append("\r\n");

        char[] buf = new char[1024];
        int len;
        while ((len = msg.read(buf)) != -1) {
            out.write(buf, 0, len);
        }
        out.flush();        

        Reader in = new InputStreamReader(socket.getInputStream());
        while ((len = in.read(buf)) != -1) {
            System.out.print(new String(buf, 0, len));
        }
        System.out.println(); // Flush
        throw new UnsupportedOperationException("TODO: Parser HTTP response");
    }
}
