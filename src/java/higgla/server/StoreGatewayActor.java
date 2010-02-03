package higgla.server;

import juglr.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Responsible for commiting Transactions
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Feb 3, 2010
 */
public class StoreGatewayActor extends Actor {

    private Queue<Transaction> todo;
    private int actualTransactionLatch;
    private Transaction actualTransaction;
    private IndexWriter actualWriter;
    private Address writer;


    public StoreGatewayActor() {
        todo = new PriorityQueue<Transaction>();
        writer = new WriterActor().getAddress();
    }

    /**
     * Commits a {@link Transaction} to the Lucene index
     * @param message
     */
    @Override
    public void react(Message message) {
        if (message instanceof Transaction) {
            handleTransaction((Transaction)message);
        } else if (message instanceof Check) {
            handleCheck((Check)message);
        } else {
            throw new MessageFormatException(
                    "Expected Storage.Transaction. Got "
                    + message.getClass().getName());
        }
    }

    private void handleCheck(Check check) {
        assert check.transactionId == actualTransaction.getId();
        actualTransactionLatch--;

        if (actualTransactionLatch == 0) {
            try {
                actualWriter.commit();
                Box reply = formatMsg(
                                Long.toString(actualTransaction.getId()), "ok");
                reply.put("transaction", actualTransaction.getId());
                send(reply, actualTransaction.getSender());
                actualTransaction = null;
                releaseWriter(actualWriter);
                scheduleNextTransaction();
            } catch (IOException e) {
                try {
                    Box reply = formatMsg(
                            Long.toString(actualTransaction.getId()),
                            "Failed to roll back transaction after failed commit: %s",
                            e.getMessage());
                    reply.put("transaction", actualTransaction.getId());
                    send(reply, actualTransaction.getSender());
                    actualWriter.rollback();
                    actualTransaction = null;
                    releaseWriter(actualWriter);
                    scheduleNextTransaction();
                } catch (IOException e1) {
                    e.printStackTrace();
                    System.err.println(
                         "Failed to roll back transaction after failed commit");
                }
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

    private void handleTransaction(Transaction transaction) {
        todo.add(transaction);
        if (!transactionOngoing()) {
            assert actualTransactionLatch == 0;
            Transaction t = todo.poll(); // Guaranteed != null
            actualTransactionLatch = t.size();
            actualTransaction = t;
            try {
                IndexWriter baseWriter = takeWriter(t.getBase());
                for (Transaction.Revision rev : actualTransaction) {
                    rev.baseWriter = baseWriter;
                    rev.transactionId = t.getId();
                    send(rev, writer);
                }
            } catch (Exception e) {
                Box error = formatMsg("error",
                        "Failed to commit transaction '%s': %s",
                        t, e.getMessage());
                error.put("transaction", t.getId());
                return;
            }
        }
    }

    private void scheduleNextTransaction() {
        if (todo.size() > 0) {

        }
    }

    private boolean transactionOngoing() {
        return actualTransaction != null;
    }

    protected Box formatMsg(String field, String format, Object... args) {
        return Box.newMap().put(field, String.format(format, args));
    }

    /**
     * Grab a reference to a writer that is ready for writing to
     * {@code indexDir}. When done with the writer you <i>must</i> call
     * {@link #releaseWriter}
     * @param base the directory to store the index in
     * @return a new or pooled writer instance
     * @throws IOException if there is an error creating the writer
     */
    protected IndexWriter takeWriter(String base) throws IOException {
        return new IndexWriter(
                         FSDirectory.open(new File(base)),
                         new StandardAnalyzer(Version.LUCENE_CURRENT, Collections.EMPTY_SET),
                         IndexWriter.MaxFieldLength.LIMITED);
    }

    /**
     * Release a writer obtained by calling {@link #takeWriter} and commit
     * any transactions it might have pending
     * @param writer the writer to release
     * @throws IOException
     */
    protected void releaseWriter(IndexWriter writer) throws IOException {
        writer.close();
    }

    private static class Check extends Message {
        public long transactionId;
        public String errorMsg;
    }

    private static class WriterActor extends Actor {
        private final Check check = new Check();

        @Override
        public void react(Message message) {
            assert message instanceof Transaction.Revision;
            Transaction.Revision rev = (Transaction.Revision)message;
            try {
                if (rev.type == Transaction.Revision.UPDATE) {
                    // FIXME: Check existence and revision
                    rev.baseWriter.addDocument(rev.doc);
                } else if (rev.type == Transaction.Revision.DELETE){
                    rev.baseWriter.deleteDocuments(new Term("__id__", rev.id));
                }
                check.transactionId = rev.transactionId;
                check.errorMsg = null;
                send(check, rev.getSender());
            } catch (IOException e) {
                check.transactionId = rev.transactionId;
                check.errorMsg = e.getMessage();
                send(check, rev.getSender());
            }
        }
    }
}
