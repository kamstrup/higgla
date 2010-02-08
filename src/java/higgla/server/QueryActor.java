package higgla.server;

import juglr.*;
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
public class QueryActor extends HigglaActor {

    private BoxParser boxParser;
    private Analyzer indexAnalyzer;

    public QueryActor() {
        super("__base__", "__query__");
        boxParser = new JSonBoxParser();
        indexAnalyzer = new StandardAnalyzer(
                                Version.LUCENE_CURRENT, Collections.EMPTY_SET);
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
        
        String base = box.getString("__base__");
        Box queryBox = box.get("__query__");

        // Parse the query
        Query query;
        try {
            query = parseQuery(queryBox);
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
        IndexReader reader;
        Box envelope = Box.newMap();
        Box results = Box.newList();
        envelope.put("__results__", results);
        try {
            searcher = takeSearcher(base);
            reader = searcher.getIndexReader();
            TopDocs docs = searcher.search(query, 10);
            for (ScoreDoc scoreDoc : docs.scoreDocs) {
                Document doc = reader.document(scoreDoc.doc);
                Box resultBox = boxParser.parse(
                                    doc.getField("__body__").stringValue());
                results.add(resultBox);
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

    public Query parseQuery (Box box) throws MessageFormatException {
        box.checkType(Box.Type.LIST);

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
