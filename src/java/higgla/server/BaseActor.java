package higgla.server;

import juglr.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Responsible for handling {@link Transaction}s for a particular base. There
 * will be one {@code BaseActor} for each base the Higgla server has written to.
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Feb 3, 2010
 */
public class BaseActor extends Actor {

    private Queue<Transaction> todo;
    private int actualTransactionLatch;
    private Transaction actualTransaction;
    private IndexWriter indexWriter;
    private Address writer;
    private Address baseAddress;
    private String baseName;
    private boolean started;

    public BaseActor(String baseName) {
        this.baseName = baseName;
        todo = new PriorityQueue<Transaction>();
    }

    @Override
    public void start() {
        try {
            baseAddress = getBus().allocateNamedAddress(this,
                                                        "__base__" + baseName);
        } catch (AddressAlreadyOwnedException e) {
            // There was a race creating this BaseActor and another actor
            // is already responsible for this base so we silently retract
            // this actor from the bus
            getBus().freeAddress(getAddress());
        }

        // We can now assume that we are the unique owner
        // of the name "__base_${baseName}"
        try {
            indexWriter = new IndexWriter(FSDirectory.open(new File(baseName)),
                                          new StandardAnalyzer(
                                                  Version.LUCENE_CURRENT,
                                                  Collections.EMPTY_SET),
                                          IndexWriter.MaxFieldLength.LIMITED);
        } catch (IOException e) {
            // Failed to open the index. Retract this actor from the bus
            shutdown();
            e.printStackTrace();
            System.err.println(String.format(
                       "Failed to create base '%s'", baseName));
        }
        
        writer = new WriterActor(indexWriter).getAddress();
        scheduleNextTransaction();
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
        assert actualTransaction != null;
        assert check.transactionId == actualTransaction.getId();
        actualTransactionLatch--;

        if (actualTransactionLatch == 0) {
            try {
                indexWriter.commit();
                Box reply = formatMsg(
                                Long.toString(actualTransaction.getId()), "ok");
                reply.put("transaction", actualTransaction.getId());
                send(reply, actualTransaction.getReplyTo());
                actualTransaction = null;
                scheduleNextTransaction();
            } catch (IOException e) {
                try {
                    Box reply = formatMsg(
                            Long.toString(actualTransaction.getId()),
                            "Failed to roll back transaction after failed commit: %s",
                            e.getMessage());
                    reply.put("transaction", actualTransaction.getId());
                    send(reply, actualTransaction.getReplyTo());
                    indexWriter.rollback();
                    actualTransaction = null;
                    scheduleNextTransaction();
                } catch (IOException e1) {
                    e.printStackTrace();
                    System.err.println(
                         "Failed to roll back transaction after failed commit");
                    shutdown();
                }
            }
        }
    }

    private void handleTransaction(Transaction transaction) {
        todo.add(transaction);

        // If writer==null start() has not completed yet
        if (!transactionOngoing() && writer != null) {
            scheduleNextTransaction();
        }
    }

    private void shutdown() {
        send(WriterActor.SHUTDOWN, writer);
        getBus().freeAddress(baseAddress);
        getBus().freeAddress(getAddress());
        try {
            indexWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println(String.format(
                    "Error shutting down base '%s'", baseName));
        }
    }

    private void scheduleNextTransaction() {
        assert actualTransaction == null;
        assert actualTransactionLatch == 0;

        Transaction t = todo.poll();
        if (t != null) {
            actualTransactionLatch = t.size();
            actualTransaction = t;
            for (Transaction.Revision rev : actualTransaction) {
                send(rev, writer);
            }
        }
    }

    private boolean transactionOngoing() {
        return actualTransaction != null;
    }

    protected Box formatMsg(String field, String format, Object... args) {
        return Box.newMap().put(field, String.format(format, args));
    }

    private static class Check extends Message {
        public long transactionId;
        public String errorMsg; // If set this Check indicates an error
    }

    private static class WriterActor extends Actor {
        public static final Message SHUTDOWN = new Message();
        private final Check check = new Check();
        private IndexWriter indexWriter;
        private BoxReader boxReader;

        public WriterActor(IndexWriter indexWriter) {
            this.indexWriter = indexWriter;
            boxReader = new JSonBoxReader(new Box(true));
        }

        @Override
        public void react(Message message) {
            if (message == SHUTDOWN) {
                getBus().freeAddress(getAddress());
                return;
            }

            assert message instanceof Transaction.Revision;
            Transaction.Revision rev = (Transaction.Revision)message;
            try {
                if (rev.type == Transaction.Revision.UPDATE) {
                    // FIXME: Check existence and revision
                    Document doc = boxToDocument(rev.box);
                    indexWriter.addDocument(doc);
                } else if (rev.type == Transaction.Revision.DELETE){
                    indexWriter.deleteDocuments(new Term("__id__", rev.id));
                }
                check.transactionId = rev.transactionId;
                check.errorMsg = null;
                send(check, rev.getReplyTo());
            } catch (IOException e) {
                check.transactionId = rev.transactionId;
                check.errorMsg = e.getMessage();
                send(check, rev.getReplyTo());
            }
        }

        private Document boxToDocument(Box box) {
            Document doc = new Document();
            String id = box.getString("__id__");
            long rev = box.getLong("__rev__");
            String body = boxReader.reset(box).asString();

            // Add stored fields
            doc.add(new Field(
                    "__id__", id, Field.Store.YES, Field.Index.NOT_ANALYZED));
            doc.add(new NumericField(
                    "__rev__", Field.Store.YES, true).setLongValue(rev));
            doc.add(new Field(
                    "__body__", body, Field.Store.YES, Field.Index.NO));

            // Indexed fields
            List<Box> indexFields;
            if (box.has("__index__")) {
                indexFields = box.getList("__index__");
            } else {
                indexFields = Collections.EMPTY_LIST;
            }
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

    }
}
