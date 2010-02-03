package higgla.server;

import juglr.Box;
import juglr.Message;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class Transaction extends Message implements
                       Comparable<Transaction>, Iterable<Transaction.Revision> {
    private String base;
    private long transactionId;
    private List<Revision> changeList;

    private static AtomicLong transactionIdCounter;

    private Transaction(String base) throws TransactionException {
        this.base = base;
        transactionId = transactionIdCounter.incrementAndGet();
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

    public long getId() {
        return transactionId;
    }

    public String getBase() {
        return base;
    }

    public int size() {
        return changeList.size();
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

    public void close() throws TransactionException {
        changeList.clear();
    }

    public int compareTo(Transaction transaction) {
        // FIXME: Overflow
        return (int)(transactionId - transaction.transactionId);
    }

    public String toString() {
        return Long.toString(transactionId);
    }

    public Iterator<Revision> iterator() {
        return changeList.iterator();
    }

    public static class Revision extends Message {
        public static final int UPDATE = 1;
        public static final int DELETE = 2;

        public int type;
        public Document doc;
        public long rev;
        public String id;

        // Used internally by StoreGatewayActor as an optimization
        public IndexWriter baseWriter;
        public long transactionId;
    }
}

