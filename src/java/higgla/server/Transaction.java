package higgla.server;

import juglr.Box;
import juglr.Message;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class Transaction extends Message implements
                       Comparable<Transaction>, Iterable<Transaction.Revision> {
    private String base;
    private long transactionId;
    private List<Revision> changeList;

    private static final AtomicLong transactionIdCounter = new AtomicLong();

    public Transaction(String base) {
        this.base = base;
        transactionId = transactionIdCounter.incrementAndGet();
        changeList = new LinkedList<Revision>();
    }

    public Transaction add(Box box) throws TransactionException {
        Revision rev = prepareRevision(box, Revision.UPDATE);
        changeList.add(rev);
        return this;
    }

    public Transaction delete(Box box) throws TransactionException {
        Revision rev = prepareRevision(box, Revision.DELETE);
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

    private Revision prepareRevision(Box box, int revType)
            throws TransactionException {
        Revision rev = new Revision();
        rev.id = box.getString("_id");
        rev.rev = box.getLong("_rev");
        rev.box = box;
        rev.type = revType;
        rev.transactionId = transactionId;

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
        public long rev;
        public String id;
        public Box box;
        public long transactionId;
    }
}

