package higgla.server;

import juglr.*;
import juglr.net.HTTP;
import juglr.net.HTTPRequest;
import juglr.net.HTTPResponse;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * FIXME: Missing class docs for higgla.server.CountActor
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Feb 19, 2010
 */
public class CountActor extends BaseActor {

    private BoxParser boxParser;
    private QueryParser queryParser;

    public CountActor(String baseName) {
        super(baseName);
        boxParser = new JSonBoxParser();
        queryParser = new QueryParser();
    }

    @Override
    public void start() {
        try {
            getBus().allocateNamedAddress(this,
                                          CountActor.baseAddress(baseName));
        } catch (AddressAlreadyOwnedException e) {
            // Another CountActor is already running for this base.
            // Retract from the bus silently
            getBus().freeAddress(getAddress());
        }
    }

    @Override
    public void react(Message message) {
        if (!(message instanceof Box)) {
            replyTo(message, HTTP.Status.InternalError, "error",
                    "Expected Box. Found '%s'", message.getClass().getName());
            return;
        }

        Box box = (Box)message;

        if (box.getType() != Box.Type.MAP) {
            replyTo(message, HTTP.Status.BadRequest, "error",
                    "Expected MAP. Got '%s'", box.getType());
            return;
        }

        if (box.getMap().size() == 0) {
            replyTo(message, HTTP.Status.BadRequest, "error",
                    "No queries in request");
            return;
        }

        IndexSearcher searcher;
        try {
            searcher = takeSearcher(baseName);
        } catch (IOException e) {
            e.printStackTrace();
            replyTo(message, HTTP.Status.InternalError, "error",
                    "Error opening searcher: %s", e.getMessage());
            return;
        }

        // Any key in the query MAP not starting with _ is to be
        // executed as a single query
        Box reply = Box.newMap();
        HTTP.Status status = HTTP.Status.OK;
        try {
            for (Map.Entry<String,Box> queryBox : box.getMap().entrySet()) {
                // FIXME: Parallelize queries
                if (queryBox.getKey().startsWith("_")) {
                    continue;
                }
                int hitCount = countHits(queryBox.getValue(), searcher);
                reply.put(queryBox.getKey(), hitCount);
            }
        } catch (MessageFormatException e) {
            reply = formatMessage("error",
                                  "Invalid message format: %s", e.getMessage());
            status = HTTP.Status.BadRequest;
        } catch (Box.TypeException e) {
            reply = formatMessage("error",
                                  "Invalid message type: %s", e.getMessage());
            status = HTTP.Status.BadRequest;
        } catch (IOException e) {
            reply = formatMessage("error",
                                  "Error executing query: %s", e.getMessage());
            status = HTTP.Status.InternalError;
        } catch (Throwable t) {
            t.printStackTrace();
            String hint = t.getMessage();
            hint = hint != null ? hint : t.getClass().getSimpleName();
            reply = formatMessage("error", "Internal error: %s", hint);
            status = HTTP.Status.InternalError;
        } finally {
            try {
                releaseSearcher(searcher);
            } catch (IOException e) {
                reply = formatMessage("error",
                                      "Error releasing searcher: %s",
                                      e.getMessage());
                status = HTTP.Status.InternalError;
            }

            send(new HTTPResponse(status, reply), message.getReplyTo());
        }
    }

    private int countHits(Box templates, IndexSearcher searcher)
                 throws MessageFormatException, Box.TypeException, IOException {
        if (templates.getList().size() == 0) {
            throw new MessageFormatException("Empty query");
        }

        Query query = queryParser.parseTemplates(templates.getList());

        // Execute query and get the hit count
        TopDocs docs = searcher.search(query, 1);
        return docs.totalHits;
    }

    private IndexSearcher takeSearcher(String base) throws IOException {
        // FIXME: Optimize this by caching the reader/searcher
        File indexDir = new File(base);
        IndexReader reader = IndexReader.open(FSDirectory.open(indexDir));
        return new IndexSearcher(reader);
    }

    private void releaseSearcher(IndexSearcher searcher) throws IOException {
        if (searcher != null) searcher.close();
    }

    public static String baseAddress(CharSequence baseName) {
        return "/_count_" + baseName;
    }
}
