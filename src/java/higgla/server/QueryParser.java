package higgla.server;

import juglr.Box;
import juglr.MessageFormatException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * FIXME: Missing class docs for higgla.server.QueryParser
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Feb 19, 2010
 */
public class QueryParser {

    private Analyzer indexedFieldAnalyzer;

    public QueryParser() {
        indexedFieldAnalyzer = new StandardAnalyzer(
                                 Version.LUCENE_CURRENT, Collections.EMPTY_SET);
    }

    public Query parseTemplates (List<Box> templates)
                                                 throws MessageFormatException {
        BooleanQuery q = new BooleanQuery();
        for (Box tmpl : templates) {
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

                            qTmpl.add(parseIndexedFieldQuery(
                                  field.name, valueBox.getString(), BooleanClause.Occur.MUST),
                                  field.occur);
                        }
                        break;
                    case MAP:
                    case LIST:
                        throw new UnsupportedOperationException("FIXME");
                }
            }
            q.add(qTmpl, BooleanClause.Occur.SHOULD);
        }

        return q;
    }

    /* Create a query on a given field by tokenizing a string with
     * the indexAnalyzer, joining all terms with the boolean op. termJoin */
    private Query parseIndexedFieldQuery(
                     String field, String query, BooleanClause.Occur termJoin) {
        BooleanQuery indexQuery = new BooleanQuery();
        try {
            TokenStream tokens = indexedFieldAnalyzer.reusableTokenStream(
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
            return new TermQuery(new Term("_error", ""));
        }

        return indexQuery;
    }


    private static class FieldSpec {
        public BooleanClause.Occur occur;
        public String name;
        public boolean isPrefix;
        public boolean isNegated;
    }

    private FieldSpec parseFieldSpec(String field) {
        FieldSpec spec = new FieldSpec();
        spec.occur = BooleanClause.Occur.MUST;
        spec.isNegated = field.startsWith("!");
        spec.isPrefix = field.endsWith("*");
        // Note: We could use other begin/end chars, like <, >, +, - etc.
        //       to define range queries etc.

        if (spec.isNegated && spec.isPrefix) {
            spec.name = field.substring(1, field.length() -1);
            spec.occur = BooleanClause.Occur.MUST_NOT;
        } else if (spec.isNegated) {
            spec.name = field.substring(1, field.length());
            spec.occur = BooleanClause.Occur.MUST_NOT;
        } else if (spec.isPrefix) {
            spec.name = field.substring(0, field.length() -1);
        } else {
            spec.name = field;
        }

        return spec;
    }
}
