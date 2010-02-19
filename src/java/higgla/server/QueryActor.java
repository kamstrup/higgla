package higgla.server;

import juglr.*;
import juglr.net.HTTP;
import juglr.net.HTTPResponse;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import static org.apache.lucene.search.BooleanClause.Occur;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Map;

/**
 * Queries a Lucene index based on Box/JSON templates. The incoming message
 * must be of Box type {@code MAP} containing a collection of queries to
 * execute. Each entry assigns a <i>query name</i> a <i>query instance</i>.
 * The query name is a free form string not starting with and underscore, _.
 * The query instance is a MAP contaning the fields:
 * <ul>
 *   <li>{@code _offset} - Integer offset into the result from which to
 *       return results. If left out, the default is 0.</li>
 *   <li>{@code _count} - An integer defining the maximum number of results to
 *       return, starting from {@code _offset}. If left out the default is
 *       20</li>
 *   <li>{@code _templates} - a LIST of Box templates results should match.
 *       A Box is considered matching if at matches at least one of the
 *       templates</li>
 * </ul>
 * <p/>
 * The QueryActor will reply to {@code box.getReplyTo()} with a
 * {@link HTTPResponse} with the body set to a Box of MAP type. The response
 * body contains one entry per named query in the request, with value
 * set to a Box with the following fields:
 * <ul>
 *   <li>{@code _total} - Total number of matching boxes. Integer</li>
 *   <li>{@code _count} - The number of boxes returned out of the total.
 *       Integer</li>
 *   <li>{@code _data} - a LIST containing {@code _count} Boxes with the
 *       retrieved results</li>
 * </ul>
 * <p/>
 * So to query all boxes with the word "john" in the
 * field "name" you would send:
 * <pre>
 *   {
 *     "myquery" : {
 *         _offset : 0,
 *         _count : 10,
 *         _templates : [{name:"john"}]
 *     }
 *   }
 * </pre>
 * If you wanted to search for "john" or "frank" in the "name" field you could
 * do:
 * <pre>
 *   {
 *     "myquery" : {
 *         _offset : 0,
 *         _count : 10,
 *         _templates : [{name:"john"}, {name:"frank"}]
 *     }
 *   }
 * </pre>
 * If you wanted a more specific John with last name "hansen", but leave the
 * query on Franks open, you simply add another field in John's template:
 * <pre>
 *   {
 *     "myquery" : {
 *         _offset : 0,
 *         _count : 10,
 *         _templates : [{name:"john", lastname:"hansen"}, {name:"frank"}]
 *     }
 *   }
 * </pre>
 * It is also possible to send more than one query in the same request. If you
 * are managing a social database where people rate books and make friends.
 * People are identified by their email addresses. Say John's email address is
 * <code>john@example.com</code> you could find John's friends and books
 * in one request:
 * <pre>
 *   {
 *     "John's books" : {
 *         _templates : [{type:"book", owner:"john@example.com"}]
 *     },
 *     "John's friends" : {
 *         _templates : [{type:"person", friend:"john@example.com"}]
 *     }
 *   }
 * </pre>
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Jan 29, 2010
 */
public class QueryActor extends BaseActor {

    private BoxParser boxParser;
    private QueryParser queryParser;

    public QueryActor(String baseName) {
        super(baseName);
        boxParser = new JSonBoxParser();
        queryParser = new QueryParser();
    }

    @Override
    public void start() {
        try {
            getBus().allocateNamedAddress(this,
                                          QueryActor.baseAddress(baseName));
        } catch (AddressAlreadyOwnedException e) {
            // Another QueryActor is already running for this base.
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
                Box result = executeQuery(queryBox.getValue(), searcher);
                reply.put(queryBox.getKey(), result);
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

    private Box executeQuery(Box queryBox, IndexSearcher searcher)
                 throws MessageFormatException, Box.TypeException, IOException {
        if (queryBox.getMap().size() == 0) {
            return formatMessage("error", "Empty query");
        }

        Box templates = queryBox.get("_templates");
        int offset = (int)queryBox.getLong("_offset", 0);
        int count = (int)queryBox.getLong("_count", 20);

        Query query = queryParser.parseTemplates(templates.getList());

        // Execute query, collect __body__ fields, parse them as Boxes,
        // and return to sender
        Box envelope = Box.newMap();
        Box results = Box.newList();
        TopDocs docs = searcher.search(query, offset+count);

        for (ScoreDoc scoreDoc : docs.scoreDocs) {
            if (--offset >= 0) continue; // skip the first 'offset'-hits

            Document doc = searcher.doc(scoreDoc.doc);
            Box resultBox = boxParser.parse(
                    doc.getField("_body").stringValue());
            results.add(resultBox);
        }
        envelope.put("_count", results.size());
        envelope.put("_total", docs.totalHits);
        envelope.put("_data", results);

        return envelope;
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



    public static String baseAddress(CharSequence base) {
        return "/_query_"+base;
    }    
}
