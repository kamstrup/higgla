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
public class QueryActor extends BaseActor {

    private BoxParser boxParser;
    private Analyzer indexAnalyzer;

    public QueryActor(String baseName) {
        super(baseName);
        boxParser = new JSonBoxParser();
        indexAnalyzer = new StandardAnalyzer(
                                Version.LUCENE_CURRENT, Collections.EMPTY_SET);
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

        // Any key in the query MAP not starting with _ is to be
        // executed as a single query
        // {
        //   queryName1 : list of box templates to match
        //   queryName2 : list of box templates to match
        //   _privateField1 : stuff
        //   ...
        // }
        // Our response looks like:
        // {
        //    queryName1 : list of results
        //    queryName2 : list of result
        // }
        //

        IndexSearcher searcher;
        try {
            searcher = takeSearcher(baseName);
        } catch (IOException e) {
            e.printStackTrace();
            replyTo(message, HTTP.Status.InternalError, "error",
                    "Error opening searcher: %s", e.getMessage());
            return;
        }

        Box reply = Box.newMap();
        try {
            for (Map.Entry<String,Box> queryBox : box.getMap().entrySet()) {
                // FIXME: Parallelize queries
                Box result = executeQuery(queryBox.getValue(), searcher);
                reply.put(queryBox.getKey(), result);
            }
        } catch (MessageFormatException e) {
            reply = formatMessage("error",
                                  "Invalid message format: %s", e.getMessage());
        } catch (Box.TypeException e) {
            reply = formatMessage("error",
                                  "Invalid message type: %s", e.getMessage());
        } catch (IOException e) {
            reply = formatMessage("error",
                                  "Error executing query: %s", e.getMessage());
        } catch (Throwable t) {
            t.printStackTrace();
            String hint = t.getMessage();
            hint = hint != null ? hint : t.getClass().getSimpleName();
            reply = formatMessage("error", "Internal error: %s", hint);
        } finally {
            try {
                releaseSearcher(searcher);
            } catch (IOException e) {
                reply = formatMessage("error",
                                      "Error releasing searcher: %s",
                                      e.getMessage());
            }

            send(new HTTPResponse(HTTP.Status.OK, reply), message.getReplyTo());
        }





    }

    private Box executeQuery(Box queryBox, IndexSearcher searcher)
                 throws MessageFormatException, Box.TypeException, IOException {
        if (queryBox.getList().size() == 0) {
            return formatMessage("error", "Empty query");
        }

        Query query = parseQuery(queryBox);

        // Execute query, collect __body__ fields, parse them as Boxes,
        // and return to sender
        Box results = Box.newList();
        searcher = takeSearcher(baseName);
        TopDocs docs = searcher.search(query, 10);
        for (ScoreDoc scoreDoc : docs.scoreDocs) {
            Document doc = searcher.doc(scoreDoc.doc);
            Box resultBox = boxParser.parse(
                    doc.getField("__body__").stringValue());
            results.add(resultBox);
        }

        return results;
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
        BooleanQuery q = new BooleanQuery();
        for (Box tmpl : box.getList()) {
            tmpl.checkType(Box.Type.MAP);
            BooleanQuery qTmpl = new BooleanQuery();
            for (Map.Entry<String,Box> entry : tmpl.getMap().entrySet()) {
                // FIXME: handle nested objects, right now we require a string, see TODO file
                FieldSpec field = parseFieldSpec(entry.getKey());

                Box valueBox = entry.getValue();
                switch (valueBox.getType()) {
                    case INT:
                        long lval = valueBox.getLong();
                        qTmpl.add(NumericRangeQuery.newLongRange(
                                field.name, lval, lval, true, true), field.occur);
                        break;
                    case FLOAT:
                        double dval = valueBox.getFloat();
                        qTmpl.add(NumericRangeQuery.newDoubleRange(
                                field.name, dval, dval, true, true), field.occur);
                        break;
                    case BOOLEAN:
                        qTmpl.add(new TermQuery(new Term(
                                field.name, valueBox.toString())), field.occur);
                        break;
                    case STRING:
                        if (field.isPrefix) {
                            qTmpl.add(new PrefixQuery(new Term(
                               field.name, valueBox.getString())), field.occur);
                        } else {

                            qTmpl.add(parseIndexQuery(
                                  field.name, valueBox.getString(), Occur.MUST),
                                  field.occur);
                        }
                        break;
                    case MAP:
                    case LIST:
                        throw new UnsupportedOperationException("FIXME");
                }
            }
            q.add(qTmpl, Occur.SHOULD);
        }

        return q;
    }

    /* Create a query on a given field by tokenizing a string with
     * the indexAnalyzer, joining all terms with the boolean op. termJoin */
    private Query parseIndexQuery(String field, String query, Occur termJoin) {
        BooleanQuery indexQuery = new BooleanQuery();
        try {
            TokenStream tokens = indexAnalyzer.reusableTokenStream(
                                                   "", new StringReader(query));
            while(tokens.incrementToken()) {
                TermAttribute term = tokens.getAttribute(TermAttribute.class);
                indexQuery.add(
                        new TermQuery(new Term(field, term.term())), termJoin);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println(
                          "I/O error parsing query. This should never happen");
            return new TermQuery(new Term("__error__", ""));
        }

        return indexQuery;
    }

    public static String baseAddress(CharSequence base) {
        return "/_query_"+base;
    }

    private static class FieldSpec {
        public Occur occur;
        public String name;
        public boolean isPrefix;
        public boolean isNegated;
    }

    private FieldSpec parseFieldSpec(String field) {
        FieldSpec spec = new FieldSpec();
        spec.occur = Occur.MUST;
        spec.isNegated = field.startsWith("!");
        spec.isPrefix = field.endsWith("*");
        // Note: We could use other begin/end chars, like <, >, +, - etc.
        //       to define range queries etc.

        if (spec.isNegated && spec.isPrefix) {
            spec.name = field.substring(1, field.length() -1);
            spec.occur = Occur.MUST_NOT;
        } else if (spec.isNegated) {
            spec.name = field.substring(1, field.length());
            spec.occur = Occur.MUST_NOT;
        } else if (spec.isPrefix) {
            spec.name = field.substring(0, field.length() -1);
        } else {
            spec.name = field;
        }

        return spec;
    }
}
