package higgla.server;

import juglr.*;
import juglr.net.HTTP;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Fetches a collection of boxes given their ids. The incoming request
 * must contain a field named {@code __ids__} which is a list containing the
 * ids of the boxes to look up. Other than the  {@code __list__} field one must
 * also specify the base in which to query by setting the {@code __base__}
 * field to the name of the base to look in.
 * <p/>
 * The return value is map that contains a {@code __result__} field with a list
 * of all retrieved boxes <i>in the same order as specified by the
 * {@code __ids__}</i> field in the query.
 * <p/>
 * So a sample get request could look like:
 * <pre>
 *   { "__base__" : "mybase",
 *     "__ids__" : ["mydoc1", "mydoc2"] }
 * </pre>
 * which would return:
 * <pre>
 *   { "__results__" : [
 *       { "__id__" : "mydoc1", "__base__" : "mybase", "myfield1" : "myvalue1", ... },
 *       { "__id__" : "mydoc2", "__base__" : "mybase", "myfield2" : "myvalue2", ... }
 *     ]
 *   }
 * </pre>
 * <i>Failed look ups:</i> In case a box can not be found for a given id an
 * empty box, <code>{}</code>, is returned in its place.
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Feb 1, 2010
 */
public class GetActor extends BaseActor {

    private BoxParser boxParser;

    public GetActor(String baseName) {
        super(baseName);
        boxParser = new JSonBoxParser();
    }

    @Override
    public void start() {
        try {
            getBus().allocateNamedAddress(this,
                                          GetActor.baseAddress(baseName));
        } catch (AddressAlreadyOwnedException e) {
            // Another GetActor is already running for this base.
            // Retract from the bus silently
            getBus().freeAddress(getAddress());
        }
    }

    public static String baseAddress(String baseName) {
        return "/_get_" + baseName;
    }

    @Override
    public void react(Message message) {
         if (!(message instanceof Box)) {
            replyTo(message, HTTP.Status.InternalError, "error",
                    "Expected Box. Found '%s'", message.getClass().getName());
            return;
        }

        Box box = (Box)message;
        if (box.getType() != Box.Type.LIST) {
            replyTo(message, HTTP.Status.BadRequest, "error",
                    "Expected LIST. Got '%s'", box.getType());
            return;
        }

        List<Box> ids = box.getList();
        if (ids.size() == 0) {
            replyTo(message, HTTP.Status.BadRequest, "error",
                    "No ids specified in request");
            return;
        }

        // Parse the query
        Query[] queries;
        try {
            queries = parseQueries(ids);
        } catch (MessageFormatException e) {
            replyTo(message, HTTP.Status.BadRequest,
                    "error", "Invalid message format: %s", e.getMessage());
            return;
        } catch (Box.TypeException e) {
            replyTo(message, HTTP.Status.BadRequest,
                    "error", "Invalid message type: %s", e.getMessage());
            return;
        }

        // Execute query, collect _body fields, parse them as Boxes,
        // and return to sender
        IndexSearcher searcher = null;
        Box results = Box.newList();
        try {
            searcher = takeSearcher();
            for (int i = 0; i < ids.size(); i++) {
                TopDocs docs = searcher.search(queries[i], 1);
                if (docs.scoreDocs.length == 0) {
                    results.add("{}");
                } else {
                    Document doc = searcher.doc(docs.scoreDocs[0].doc);
                    Box resultBox = boxParser.parse(
                                        doc.getField("_body").stringValue());
                    results.add(resultBox);
                }
            }
            send(results, message.getReplyTo());
        } catch (IOException e) {
            replyTo(message, HTTP.Status.InternalError,
                    "error", "Error executing query: %s", e.getMessage());
        } catch (Throwable t) {
            t.printStackTrace();
            String hint = t.getMessage();
            hint = hint != null ? hint : t.getClass().getSimpleName();
            replyTo(message, HTTP.Status.InternalError,
                    "error", "Internal error: %s", hint);
        } finally {
            try {
                releaseSearcher(searcher);
            } catch (IOException e) {
                replyTo(message, HTTP.Status.InternalError,
                  "error", "Error releasing searcher: %s", e.getMessage());
            }
        }
    }

    private IndexSearcher takeSearcher() throws IOException {
        // FIXME: Optimize this by caching the reader/searcher
        File indexDir = new File(baseName);
        IndexReader reader = IndexReader.open(FSDirectory.open(indexDir));
        return new IndexSearcher(reader);
    }

    private void releaseSearcher(IndexSearcher searcher) throws IOException {
        if (searcher != null) searcher.close();
    }

    private Query[] parseQueries(List<Box> ids) {
        Query[] q = new Query[ids.size()];

        for (int i = 0; i < ids.size(); i++) {
            q[i] = new TermQuery(new Term("_id", ids.get(i).getString()));
        }
        
        return q;
    }
}
