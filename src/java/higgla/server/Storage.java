package higgla.server;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

    public Transaction openTransaction(String base) {
        return null;
    }

    public void closeTransaction(Transaction t, boolean commit) {

    }

    public IndexSearcher takeSearcher(String base) {
        return null;
    }

    public void releaseSearcher(IndexSearcher searcher) {

    }

    public class Transaction {}/*
        private String base;
        private IndexWriter w;
        private Directory data;

        private Transaction(String base, Analyzer analyzer)
                                                   throws TransactionException {
            this.base = base;
            data = new RAMDirectory();
            try {
                w = new IndexWriter(data, analyzer,
                                    IndexWriter.MaxFieldLength.LIMITED);
            } catch (IOException e) {
                throw new TransactionException(String.format(
                                    "Failed to open transaction on '%s': %s",
                                    base, e.getMessage()), e);
            }
        }

        public void addDocument(Document doc) throws TransactionException {
            try {
                w.addDocument(doc);
            } catch (IOException e) {
                throw new TransactionException(
                 "Failed adding document to transaction: " + e.getMessage(), e);
            }
        }

        public void commit() throws TransactionException {
            IndexWriter index = getIndexWriter(base);
            index.addIndexesNoOptimize(data);
        }
    }

    public static class TransactionException extends Exception {
        public TransactionException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }*/
}
