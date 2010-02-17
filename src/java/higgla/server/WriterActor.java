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
 * This is ensured by the base actor grabbing the named address
 * {@code "__base__<baseName>"} when {@code start()} is called. Since only
 * one actor can own an address at any given time this ensures that we have
 * maximally one WriterActor for each base name.
 * <p/>
 * The base actor is self-healing in a sense. When it crashes it releases its
 * well known name, {@code "__base__<baseName>"}. And since the StoreActor
 * looks up the relevant WriterActor on each incoming {@link Transaction}
 * and creates the relevant WriterActor if it is not found, the WriterActor will
 * be "lazily" recreated in case of a crash.
 *
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Feb 3, 2010
 */
public class WriterActor extends Actor {

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
    private Box actualTransactionRevisions;
    private IndexWriter indexWriter;
    private IndexReader indexReader;
    private Address writer;
    private Address baseAddress;
    private String baseName;
    private Directory baseDir;
    private boolean started;
    private AtomicLong revisionCounter;

    public WriterActor(String baseName) {
        this.baseName = baseName;
        todo = new PriorityQueue<Transaction>();

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
            // There was a race creating this WriterActor and another actor
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
     * Commits a {@link Transaction} to the Lucene index and handles a Check
     * messages from one of the child WriterActors
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

        actualTransactionRevisions.put(check.boxId, check.boxRevision);

        if (check.error != null) {
            actualTransactionErrors.add(check.error);
        }

        if (actualTransactionLatch == 0) {
            // Reset and prepare for next transaction
            Transaction closingTransaction = actualTransaction;
            List<Box> closingTransactionErrors = actualTransactionErrors;
            Box closingTransactionRevisions = actualTransactionRevisions;
            actualTransaction = null;
            actualTransactionErrors = null;
            actualTransactionRevisions = null;

            if (closingTransactionErrors.size() != 0) {
                try {
                    indexWriter.rollback();
                    renewWriter();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.err.println(String.format(
                         "I/O Error while rolling back transaction '%s': %s",
                         closingTransaction.getId(), e.getMessage()));
                } finally {
                    Box reply = formatMsg(
                            Long.toString(closingTransaction.getId()), "error");
                    reply.put("transaction", closingTransaction.getId());
                    reply.put("error", closingTransactionErrors);
                    reply.put("revisions", closingTransactionRevisions);
                    send(reply, closingTransaction.getReplyTo());
                    
                    renewWriter();
                }
                return;
            }

            try {
                // Commit and notify
                indexWriter.commit();
                commitMeta();
                Box reply = formatMsg(
                                Long.toString(closingTransaction.getId()), "ok");
                reply.put("transaction", closingTransaction.getId());
                reply.put("revisions", closingTransactionRevisions);
                send(reply, closingTransaction.getReplyTo());

                // Reply has been send; now reload the reader to make sure
                // it sees up to date revisions and ids
                renewReader();

                scheduleNextTransaction();
            } catch (IOException e) {
                Box reply = formatMsg(
                            "error", "Failed to commit transaction '%s': %s",
                            closingTransaction.getId(), e.getMessage());
                    reply.put(Long.toString(closingTransaction.getId()), "error");
                    reply.put("transaction", closingTransaction.getId());
                try {
                    indexWriter.rollback();
                    scheduleNextTransaction();
                } catch (IOException e1) {
                    e.printStackTrace();
                    System.err.println(
                         "Failed to roll back transaction after failed commit");
                    shutdown();
                } finally {
                    send(reply, closingTransaction.getReplyTo());
                }
            }
        }
    }

    private void renewReader() throws IOException {
        if (indexReader != null) {
            // Note that the read-only mode of the original reader is inherited
            IndexReader newIndexReader = indexReader.reopen();
            if (newIndexReader != indexReader) {
                // Reader was reopened
                indexReader.close();
                indexReader = newIndexReader;
            }
        } else {
            // Open the reader in read-only mode
            indexReader = IndexReader.open(baseDir, true);
        }
    }

    private void renewWriter() {
        assert revisionCounter != null;

        if (writer != null) {
            send(WriterDelegate.SHUTDOWN, writer);
        }
        if (indexWriter != null) {
            try {
                indexWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println(String.format(
                    "I/O Error renewing index writer for base '%s'", baseName));
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
                       "I/O error creating base '%s'", baseName));
        }

        try {
            renewReader();
        } catch (IOException e) {
            // Failed to open the reader. Retract this actor from the bus
            shutdown();
            e.printStackTrace();
            System.err.println(String.format(
                       "I/O error reading base data for '%s'", baseName));
        }

        writer = new WriterDelegate(
                     indexWriter, indexReader, revisionCounter).getAddress();
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
        send(WriterDelegate.SHUTDOWN, writer);
        getBus().freeAddress(baseAddress);
        getBus().freeAddress(getAddress());
        try {
            indexWriter.close();
            indexReader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println(String.format(
                    "Error shutting down base '%s'", baseName));
        }
    }

    private void scheduleNextTransaction() {
        assert actualTransaction == null : "Previous transaction not cleared";
        assert actualTransactionLatch == 0 : "Transaction latch not cleared";
        assert actualTransactionErrors == null : "Transaction errors remain";
        assert actualTransactionRevisions == null : "Transaction revisions not reset";

        Transaction t = todo.poll();
        if (t != null) {
            actualTransactionLatch = t.size();
            actualTransaction = t;
            actualTransactionRevisions = Box.newMap();
            actualTransactionErrors = new LinkedList<Box>();
            try {
                renewReader();
            } catch (IOException e) {
                // Print error, put transaction back in queue, and shutdown.
                // Next time a transaction is send our way the StoreActor
                // will re-create a WriterActor instance
                e.printStackTrace();
                System.err.println("I/O error reopening index reader");
                shutdown();
            }

            // We create a new writeractor for each transaction
            send(WriterDelegate.SHUTDOWN, writer);
            writer = new WriterDelegate(
                        indexWriter, indexReader, revisionCounter).getAddress();

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
        public long transactionId;  // Transaction id
        public Box error;           // If set this Check indicates an error
        public long boxRevision; // New rev. number
        public String boxId;        // Id of handled box
    }

    private static class WriterDelegate extends Actor {
        public static final Message SHUTDOWN = new Message();
        private IndexWriter indexWriter;
        private IndexReader indexReader;
        private BoxReader boxReader;
        private AtomicLong revisionCounter;

        public WriterDelegate(IndexWriter indexWriter,
                              IndexReader indexReader,
                              AtomicLong revisionCounter) {
            this.indexWriter = indexWriter;
            this.indexReader = indexReader;
            this.revisionCounter = revisionCounter;
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
            Check check = new Check();
            check.transactionId = rev.transactionId;
            check.boxId = rev.id;
            check.boxRevision = rev.rev;
            Term idTerm = new Term("__id__", rev.id);
            try {
                long currentRev = findRevisionNumber(idTerm);

                // If revision is specified correctly, then update it,
                // otherwise send back an error
                if (currentRev == rev.rev) {
                    long newRev = revisionCounter.incrementAndGet();
                    rev.box.put("__rev__", newRev);
                    Document doc = boxToDocument(rev.box);
                    if (currentRev > 0) {
                        // Update an exisiting document
                        if (rev.type == Transaction.Revision.UPDATE) {
                            indexWriter.updateDocument(idTerm, doc);
                        } else if (rev.type == Transaction.Revision.DELETE){
                            indexWriter.deleteDocuments(idTerm);
                        }
                    } else {
                        // This is a new document
                        indexWriter.addDocument(doc);
                    }
                    check.boxRevision = newRev;
                    check.error = null;
                } else {
                    check.error = Box.newMap()
                                         .put("__id__", rev.id)
                                         .put("__rev__", currentRev)
                                         .put("error", "conflict");
                }
            } catch (Throwable t) {
                check.transactionId = rev.transactionId;
                check.error = Box.newMap()
                                     .put("__id__", rev.id)
                                     .put("error", t.getMessage());
                t.printStackTrace();
                System.err.println("Error caught while updating index");
            } finally {
                send(check, rev.getReplyTo());
            }
        }

        private long findRevisionNumber(Term idTerm) throws IOException {
            TermDocs docs = indexReader.termDocs(idTerm);
            try {
                if (!docs.next()) {
                    return 0;
                }

                long revno;
                Document doc = indexReader.document(docs.doc());
                Fieldable f = doc.getFieldable("__rev__");
                if (f instanceof NumericField) {
                    revno = ((NumericField)f).getNumericValue().longValue();
                } else {
                    revno = Long.parseLong(f.stringValue());
                }

                if (docs.next()) {
                    System.err.println(String.format(
                            "INTERNAL ERROR: Duplicate entries for '%s'",
                            idTerm.text()));
                }


                return revno;
            } finally {
                docs.close();
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
