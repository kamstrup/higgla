package higgla.server;

import juglr.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;

import static org.apache.lucene.search.BooleanClause.Occur;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Queries a Lucene index based on a Box/JSON template. The incoming message
 * must be of Box type {@code MAP} containing a field named {@code __base__}
 * naming the base to query. The actual query is read from the field
 * {@code __query__} and this field must be a list of {@code MAP}s.
 * Each map will be treated as a template to match, joining all
 * fields in the map with {@code AND} conditions. The full list of templates
 * will joined into one {@code OR} query.
 * <p/>
 * So to query the base "mybase" for all documents with the word "john" in the
 * field "name" you would send:
 * <pre>
 *   {
 *     "__base__" : "mybase",
 *     "__query__" : [{ "name" : "john" }]
 *   }
 * </pre>
 * If you wanted to search for "john" or "frank" in the "name" field you would
 * do:
 * <pre>
 *   {
 *     "__base__" : "mybase",
 *     "__query__" : [{ "name" : "john" }, { "name" : "frank" }]
 *   }
 * </pre>
 * If you wanted a more specific John with last name "hansen", but leave the
 * query on Franks open, you simply add another field in John's template:
 * <pre>
 *   {
 *     "__base__" : "mybase",
 *     "__query__" : [{ "name" : "john", "lastname" : "hansen" },
 *                    { "name" : "frank" }]
 *   }
 * </pre>
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Jan 29, 2010
 */
public class QueryActor extends HigglaActor {

    private BoxParser boxParser;

    public QueryActor() {
        super("__base__", "__query__");
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
               message.getSender());
            return;
        }
        
        String base = box.getString("__base__");
        Box queryBox = box.get("__query__");

        // Parse the query
        Query query;
        try {
            query = parseQuery(queryBox);
        } catch (MessageFormatException e) {
            send(
               formatMsg("error", "Invalid message format: %s", e.getMessage()),
               message.getSender());
            return;
        } catch (Box.TypeException e) {
            send(
               formatMsg("error", "Invalid message type: %s", e.getMessage()),
               message.getSender());
            return;
        }

        System.out.println("QUERY " + query.toString());

        // Execute query, collect __body__ fields, parse them as Boxes,
        // and return to sender
        IndexSearcher searcher = null;
        IndexReader reader = null;
        Box result = Box.newMap();
        try {
            searcher = takeSearcher(base);
            reader = searcher.getIndexReader();
            TopDocs docs = searcher.search(query, 10);
            for (ScoreDoc scoreDoc : docs.scoreDocs) {
                Document doc = reader.document(scoreDoc.doc);
                Box resultBox = boxParser.parse(
                                    doc.getField("__body__").stringValue());
                String resultId = doc.getField("__id__").stringValue();
                result.put(resultId, resultBox);
            }
            send(result, message.getSender());
        } catch (IOException e) {
            send(
               formatMsg("error", "Error executing query: %s", e.getMessage()),
               message.getSender());
        } catch (Throwable t) {
            t.printStackTrace();
            String hint = t.getMessage();
            hint = hint != null ? hint : t.getClass().getSimpleName();
            send(
                  formatMsg("error", "Internal error: %s", hint),
                  message.getSender());
        } finally {
            try {
                releaseSearcher(searcher);
            } catch (IOException e) {
                send(
                  formatMsg("error", "Error releasing searcher: %s", e.getMessage()),
                  message.getSender());
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

    public Query parseQuery (Box box) throws MessageFormatException {
        box.checkType(Box.Type.LIST);

        BooleanQuery q = new BooleanQuery();
        for (Box tmpl : box.getList()) {
            tmpl.checkType(Box.Type.MAP);
            BooleanQuery qTmpl = new BooleanQuery();
            for (Map.Entry<String,Box> entry : tmpl.getMap().entrySet()) {
                // FIXME: handle nested objects, right now we require a string, see TODO file
                String field = entry.getKey();
                String value = entry.getValue().getString();
                qTmpl.add(new TermQuery(new Term(field, value)), Occur.MUST);
            }
            q.add(qTmpl, Occur.SHOULD);
        }

        return q;
    }
}
