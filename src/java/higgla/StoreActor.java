package higgla;

import juglr.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import static org.apache.lucene.document.Field.Store;
import static org.apache.lucene.document.Field.Index;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * And actor that stores messages in a Lucene index. The stored messages
 * <i>must</i> be of type {@link Box} and contain the mandatory fields:
 * <ul>
 *   <li>{@code __base__} : A "base" to store the message in. A base is
 *      roughly speaking a database in which you group similar stuff</li>
 *   <li>{@code __id__} : An id for the message that is unique within the
 *      base determined by the {@code __base__} field</li>
 * </ul>
 * There is an additional optional field called {@code __index__} which must
 * be of type {@code Box.Type.LIST} containing a list of field names which
 * will be indexed in the Lucene index.
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Jan 29, 2010
 */
public class StoreActor extends HigglaActor {

    private Analyzer analyzer;
    private BoxReader reader;

    public StoreActor() {
        super("__id__", "__base__");
        analyzer = new StandardAnalyzer(
                            Version.LUCENE_CURRENT, Collections.EMPTY_SET);
        reader = new JSonBoxReader(Box.newMap());
    }

    @Override
    public void react(Message message) {
        Box box;
        try {
            box = validate(message);
        } catch (MessageFormatException e) {
            send(
               formatMsg("error", "Invalid message format: %s", e.getMessage()),
               message.getSender());
            return;
        }
        
        String base = box.getString("__base__");
        String id = box.getString("__id__");
        List<String> index;
        Box _index = box.get("__index__");
        if (_index != null) {
            index = new ArrayList<String>(_index.size());
            for (Box indexField : _index.getList()) {
                index.add(indexField.getString());
            }
        } else {
            index = Collections.EMPTY_LIST;
        }

        try {
            storeBox(id, base, index, box);
        } catch (IOException e) {
            send(
             formatMsg("error", "Failed to store '%s': %s", id, e.getMessage()),
             message.getSender());
        } finally {
            send(formatMsg("stored", id), message.getSender());
        }
    }

    /**
     * Commit the message {@code box} to the store in {@code base} with the
     * id {@code id} indexing the fields named in {@code index}
     * @param id the id to store the message under
     * @param base the base in which to store the message
     * @param index a list of field names to index
     * @param box the message to store
     * @throws IOException if there is an error writing to the store
     */
    protected void storeBox(
                      String id, String base, List<String> index, Box box)
                                                            throws IOException {
        File indexDir = new File(base);
        IndexWriter writer = takeWriter(indexDir);

        try {
            reader.reset(box);
            Document doc = new Document();
            doc.add(new Field("__id__", id, Store.YES, Index.NOT_ANALYZED));
            doc.add(new Field(
                 "__body__", reader.asString(), Store.YES, Index.NOT_ANALYZED));
            for (String indexField : index) {
                Box field = box.get(indexField);
                doc.add(new Field(
                       indexField, field.toString(), Store.NO, Index.ANALYZED));
            }
            writer.addDocument(doc);
        } finally {
            releaseWriter(writer);
        }
    }

    /**
     * Grab a reference to a writer that is ready for writing to
     * {@code indexDir}. When done with the writer you <i>must</i> call
     * {@link #releaseWriter}
     * @param indexDir the directory to store the index in
     * @return a new or pooled writer instance
     * @throws IOException if there is an error creating the writer
     */
    protected IndexWriter takeWriter(File indexDir) throws IOException {
        Directory dir = FSDirectory.open(indexDir);
        if (indexDir.exists()) {
            if (!new File(indexDir, "segments.gen").exists()) {
                // Create a new empty index
                new IndexWriter(dir, analyzer, true,
                                IndexWriter.MaxFieldLength.LIMITED).close();
            }
        } else {
            // Create a new empty index
            new IndexWriter(dir, analyzer, true,
                            IndexWriter.MaxFieldLength.LIMITED).close();
        }
        return new IndexWriter(dir, analyzer,
                               false, IndexWriter.MaxFieldLength.LIMITED);
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
}
