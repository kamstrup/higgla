package higgla.server;

import juglr.*;
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
public class GetActor extends HigglaActor {

    private BoxParser boxParser;

    public GetActor() {
        super("__ids__", "__base__");
        boxParser = new JSonBoxParser();
    }

    @Override
    public void react(Message message) {
        Box box;
        try {
            box = validate(message);
        } catch (MessageFormatException e) {
            send(
               formatMsg("error", "Invalid message format: %s", e.getMessage()),
               message.getReplyTo());
            return;
        }

        // Parse the query
        String base = box.getString("__base__");
        List<Box> ids = box.getList("__ids__");
        Query[] queries;
        try {
            queries = parseQueries(ids);
        } catch (MessageFormatException e) {
            send(
               formatMsg("error", "Invalid message format: %s", e.getMessage()),
               message.getReplyTo());
            return;
        } catch (Box.TypeException e) {
            send(
               formatMsg("error", "Invalid message type: %s", e.getMessage()),
               message.getReplyTo());
            return;
        }

        // Execute query, collect __body__ fields, parse them as Boxes,
        // and return to sender
        IndexSearcher searcher = null;
        Box envelope = Box.newMap();
        Box results = Box.newList();
        envelope.put("__results__", results);
        try {
            searcher = takeSearcher(base);
            for (int i = 0; i < ids.size(); i++) {
                TopDocs docs = searcher.search(queries[i], 1);
                if (docs.scoreDocs.length == 0) {
                    results.add("{}");
                } else {
                    Document doc = searcher.doc(docs.scoreDocs[0].doc);
                    Box resultBox = boxParser.parse(
                                        doc.getField("__body__").stringValue());
                    results.add(resultBox);
                }
            }
            send(envelope, message.getReplyTo());
        } catch (IOException e) {
            send(
               formatMsg("error", "Error executing query: %s", e.getMessage()),
               message.getReplyTo());
        } catch (Throwable t) {
            t.printStackTrace();
            String hint = t.getMessage();
            hint = hint != null ? hint : t.getClass().getSimpleName();
            send(
                  formatMsg("error", "Internal error: %s", hint),
                  message.getReplyTo());
        } finally {
            try {
                releaseSearcher(searcher);
            } catch (IOException e) {
                send(
                  formatMsg("error", "Error releasing searcher: %s", e.getMessage()),
                  message.getReplyTo());
            }
        }
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

    private BooleanQuery[] parseQueries(List<Box> ids) {
        BooleanQuery[] q = new BooleanQuery[ids.size()];

        for (int i = 0; i < ids.size(); i++) {
            q[i] = new BooleanQuery();
            String id = ids.get(i).getString();
            q[i].add(new TermQuery(new Term(
                    "__id__", id)), BooleanClause.Occur.SHOULD);
        }
        
        return q;
    }
}
