package higgla.server;

import juglr.*;

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

    public StoreActor() {
        super("__store__", "__base__");
    }

    @Override
    public void react(Message message) {
        Box envelope;
        try {
            envelope = validate(message);
        } catch (MessageFormatException e) {
            send(
               formatMsg("error", "Invalid message format: %s", e.getMessage()),
               message.getReplyTo());
            return;
        }

        Box list = envelope.get("__store__");
        String base = envelope.getString("__base__");

        if (list.getType() != Box.Type.LIST) {
            send(
                formatMsg("error",
                    "__store__ field must be a list, found %s", list.getType()),
                 message.getReplyTo());
            return;
        }

        Transaction transaction = new Transaction(base);
        transaction.setReplyTo(message.getReplyTo());
        try {
            for (Box box : list.getList()) {
                if (box.has("__delete__")) {
                    transaction.delete(box);
                } else {
                    transaction.add(box);
                }
            }
        } catch (TransactionException e) {
            send(formatMsg("error", e.getMessage()), message.getReplyTo());
            return;
        } catch (Box.TypeException e) {
            send(formatMsg("error", e.getMessage()), message.getReplyTo());
            return;
        } catch (MessageFormatException e) {
            send(formatMsg("error", e.getMessage()), message.getReplyTo());
            return;
        }
        sendToBase(transaction, base);
    }

    private void sendToBase(Transaction transaction, String base) {
        // We look up the base actor on each transaction because the base
        // actor detaches from the bus on critical errors. Since we
        // create the actor in case it's not found this means that
        // we will dynamically recreate the base actor if it dies
        Address baseAddress = getBus().lookup("/__base__"+base);
        if (baseAddress == null) {
            baseAddress = new WriterActor(base).getAddress();
            getBus().start(baseAddress);
        }
        send(transaction, baseAddress);
    }
}
