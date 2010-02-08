package higgla.server;

import juglr.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Responsible for handling {@link Transaction}s for a particular base. There
 * will be one {@code BaseActor} for each base the Higgla server has written to.
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Feb 3, 2010
 */
public class BaseActor extends Actor {

    /** The higgla.meta file must always be exactly 2048 bytes in order
     * to guard against disk-full scenarios.
     * We also reserve a byte array of that size in memory to provide
     * durability against OOM. */
    private static final int META_SIZE = 2048;
    private static final int HIGGLA_META_VERSION = 1;
    private ByteBuffer metaBuffer;
    private File metaFile;

    private Queue<Transaction> todo;
    private int actualTransactionLatch;
    private Transaction actualTransaction;
    private List<Box> actualTransactionErrors;
    private IndexWriter indexWriter;
    private Address writer;
    private Address baseAddress;
    private String baseName;
    private Directory baseDir;
    private boolean started;
    private AtomicLong revisionCounter;

    public BaseActor(String baseName) {        
        this.baseName = baseName;
        todo = new PriorityQueue<Transaction>();
        actualTransactionErrors = new LinkedList<Box>();

        // Pre-allocate resources for the higgla.meta file
        metaBuffer = ByteBuffer.allocate(META_SIZE);
        metaFile = new File(baseName, "higgla.meta");
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
            return;
        }
        try {
            baseDir = FSDirectory.open(new File(baseName));
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("I/O Error opening base directory");
            shutdown();
            return;
        }

        // We can now assume that we are the unique owner
        // of the name "__base__${baseName}". Thus it should be safe to create
        // an IndexWriter for the base

        try {
            revisionCounter = new AtomicLong(readLastRevision());
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("I/O Error detecting last revision number");
            shutdown();
            return;
        }

        renewWriter();  // requires revisionCounter to be set
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

        if (check.error != null) {
            actualTransactionErrors.add(check.error);
        }

        if (actualTransactionLatch == 0) {
            if (actualTransactionErrors.size() != 0) {
                try {
                    indexWriter.rollback();
                    renewWriter();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.err.println(String.format(
                         "I/O Error while rolling back transaction '%s': %s",
                         actualTransaction.getId(), e.getMessage()));
                } finally {
                    Box reply = formatMsg(
                            Long.toString(actualTransaction.getId()), "error");
                    reply.put("transaction", actualTransaction.getId());
                    reply.put("error", actualTransactionErrors);
                    send(reply, actualTransaction.getReplyTo());

                    // Reset and prepare for next transaction. Note that we can
                    // not run actualTransactionErrors.clear() since this list
                    // is now owned by the reply message. Instead we create a
                    // new list
                    actualTransaction = null;
                    actualTransactionErrors = new LinkedList<Box>();
                    renewWriter();
                }
                return;
            }

            try {
                indexWriter.commit();
                commitMeta();
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

    private void renewWriter() {
        assert revisionCounter != null;

        if (writer != null) {
            send(WriterActor.SHUTDOWN, writer);
        }
        if (indexWriter != null) {
            try {
                indexWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("I/O Error renewing index writer");
            }
        }

        try {
            indexWriter = new IndexWriter(baseDir,
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

        writer = new WriterActor(indexWriter, revisionCounter).getAddress();
        scheduleNextTransaction();
    }

    /**
     * Write index metadata to the file higgla.meta - most notably our
     * revision number.
     * <p/>
     * The higgla.meta file keeps it's file format version in an integer
     * in the first 4 bytes. The higgla.meta format version 1 simply
     * stores the last known revision number as a long in the next 8 bytes
     * @throws IOException bad bad bad
     */
    private void commitMeta() throws IOException {
        metaBuffer.clear();
        metaBuffer.putInt(HIGGLA_META_VERSION);
        metaBuffer.putLong(revisionCounter.get());

        // Pad the file to META_SIZE
        while (metaBuffer.remaining() > 0) {
            metaBuffer.putInt(0);
        }
        metaBuffer.flip();

        FileChannel f = new FileOutputStream(metaFile).getChannel();
        try {
            f.write(metaBuffer);
        } finally {
            f.close();
        }
    }

    /**
     * Read the last known revision from the higgla.meta file or return 0
     * if the file doesn't exist
     * @return
     * @throws IOException
     */
    private long readLastRevision() throws IOException {
        if (!metaFile.exists()) {
            return 0;
        }

        metaBuffer.clear();
        FileChannel f = new FileInputStream(metaFile).getChannel();
        f.read(metaBuffer);
        metaBuffer.flip();

        int metaFileVersion = metaBuffer.getInt();
        if (metaFileVersion != HIGGLA_META_VERSION) {
            throw new IOException(
                    "Unsupported version number found in "+metaFile);
        }
        long lastRev = metaBuffer.getLong();
        return lastRev;
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
        assert actualTransactionErrors.size() == 0;

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
        public Box error; // If set this Check indicates an error
    }

    private static class WriterActor extends Actor {
        public static final Message SHUTDOWN = new Message();
        private final Check check = new Check();
        private IndexWriter indexWriter;
        private BoxReader boxReader;
        private AtomicLong revisionCounter;
        private Box internalError; // Returned on uncaught internal errors

        public WriterActor(IndexWriter indexWriter, AtomicLong revisionCounter){
            this.indexWriter = indexWriter;
            this.revisionCounter = revisionCounter;
            boxReader = new JSonBoxReader(new Box(true));
            internalError = Box.newMap().put("error", "Internal error");
        }

        @Override
        public void react(Message message) {
            if (message == SHUTDOWN) {
                getBus().freeAddress(getAddress());
                return;
            }

            assert message instanceof Transaction.Revision;
            Transaction.Revision rev = (Transaction.Revision)message;
            check.transactionId = rev.transactionId;
            check.error = internalError.put("__id__", rev.id);
            Term idTerm = new Term("__id__", rev.id);
            try {
                long revno = findRevisionNumber(idTerm);

                // If revision is specified correctly, then update it,
                // otherwise send back an error
                if (revno == rev.rev) {
                    Document doc = boxToDocument(
                                    rev.box, revisionCounter.incrementAndGet());
                    if (revno > 0) {
                        // Update an exisiting document
                        if (rev.type == Transaction.Revision.UPDATE) {
                            indexWriter.deleteDocuments(idTerm);
                            indexWriter.addDocument(doc);
                        } else if (rev.type == Transaction.Revision.DELETE){
                            indexWriter.deleteDocuments(idTerm);
                        }
                    } else {
                        // This is a new document
                        indexWriter.addDocument(doc);
                    }
                    check.error = null;
                } else {
                    check.error = Box.newMap()
                                         .put("__id__", rev.id)
                                         .put("__rev__", revno)
                                         .put("error", "conflict");
                }
            } catch (IOException e) {
                check.transactionId = rev.transactionId;
                check.error = Box.newMap()
                                     .put("__id__", rev.id)
                                     .put("error", e.getMessage());
            } finally {
                send(check, rev.getReplyTo());
            }
        }

        private long findRevisionNumber(Term idTerm) throws IOException {
            IndexReader r = indexWriter.getReader();
            TermDocs docs = r.termDocs(idTerm);
            try {
                if (!docs.next()) {
                    return 0;
                }

                long revno;
                Document doc = r.document(docs.doc());
                Fieldable f = doc.getFieldable("__rev__");
                if (f instanceof NumericField) {
                    revno = ((NumericField)f).getNumericValue().longValue();
                } else {
                    revno = Long.parseLong(f.stringValue());
                }

                if (docs.next()) {
                    System.err.println(String.format(
                            "INTERNAL ERROR: Duplicate entries for '%s'", idTerm.text()));
                }


                return revno;
            } finally {
                docs.close();
            }
        }

        private Document boxToDocument(Box box, long rev) {
            Document doc = new Document();
            String id = box.getString("__id__");
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

                /* Fields marked for indexing are not necessarily */
                if (value == null) {
                    continue;
                }

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
