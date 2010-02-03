package higgla.server;

import juglr.Box;
import juglr.Message;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;

import java.util.*;

/**
 * Encapsulation of shared storage. All methods are thread safe.
 */
public class Storage {

    private static final Storage defaultStorage = new Storage();

    private Map<String,IndexWriter> writerByBase;

    public static Storage getDefault() {
        return defaultStorage;
    }

    protected Storage() {
        writerByBase = Collections.synchronizedMap(
                                         new HashMap<String,IndexWriter>());        
    }

    public Transaction openTransaction(String base)
                                               throws TransactionException {
        return new Transaction(base);
    }

    private void closeTransaction(Transaction t, boolean commit)
                                               throws TransactionException {
        if (!commit) {
            return;
        }

        
    }

    public IndexSearcher takeSearcher(String base) {
        return null;
    }

    public void releaseSearcher(IndexSearcher searcher) {

    }

    private Document boxToDocument(Box box) {
        Document doc = new Document();
        String id = box.getString("__id__");
        long rev = box.getLong("__rev__");
        String body = box.get("__body__").toString();
        List<Box> indexFields;
        if (box.has("__index__")) {
            indexFields = box.getList("__index__");
        } else {
            indexFields = Collections.EMPTY_LIST;
        }

        doc.add(new Field(
                    "__id__", id, Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new NumericField(
                "__rev__", Field.Store.YES, true).setLongValue(rev));
        doc.add(new Field(
                "__body__", body, Field.Store.YES, Field.Index.NO));
        for (Box fieldBox : indexFields) {
            String field = fieldBox.getString();
            Box value = box.get(field);
            switch (value.getType()) {
                case INT:
                    doc.add(new NumericField(field).setLongValue(
                                    value.getLong()));
                    break;
                case FLOAT:
                    doc.add(
                            new NumericField(field).setDoubleValue(
                                    value.getFloat()));
                    break;
                case BOOLEAN:
                    doc.add(new Field(field, value.toString(),
                                     Field.Store.NO, Field.Index.NOT_ANALYZED));
                    break;
                case STRING:
                    doc.add(new Field(field, value.getString(),
                                      Field.Store.NO, Field.Index.ANALYZED));
                    break;
                case MAP:
                case LIST:
                    throw new UnsupportedOperationException("FIXME");
            }
        }
        return doc;
    }

    public class Transaction extends Message {
        private String base;
        private List<Revision> changeList;

        private Transaction(String base) throws TransactionException {
            this.base = base;
            changeList = new LinkedList<Revision>();
        }

        public Transaction add(Box box) throws TransactionException {
            Revision rev = prepareRevision(box, null, Revision.UPDATE);
            changeList.add(rev);
            return this;
        }

        public Transaction delete(Box box) throws TransactionException {
            Revision rev = prepareRevision(box, null, Revision.DELETE);
            changeList.add(rev);
            return this;
        }

        private Revision prepareRevision(Box box, Document doc, int revType)
                                                   throws TransactionException {
            if (!base.equals(box.getString("__base__"))) {
                throw new TransactionException(String.format(
                     "Illegal base '%s' for box '%s'. Expected '%s'",
                     box.getString("__base__"), box.getString("__id__"), base));
            }

            Revision rev = new Revision();
            rev.doc = doc;
            rev.id = box.getString("__id__");
            rev.rev = box.getLong("__rev__");
            rev.type = revType;

            return rev;
        }

        public void commit() throws TransactionException {
            Storage.this.closeTransaction(this, true);
        }

        public void close() throws TransactionException {
            changeList.clear();
            Storage.this.closeTransaction(this, false);
        }
    }

    public static class TransactionException extends Exception {
        public TransactionException(String msg, Throwable cause) {
            super(msg, cause);
        }

        public TransactionException(String msg) {
            super(msg);
        }
    }

    private static class Revision {
        public static final int UPDATE = 1;
        public static final int DELETE = 2;

        public int type;
        public Document doc;
        public long rev;
        public String id;
    }
}
