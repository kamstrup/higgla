package higgla.server;

import juglr.*;
import juglr.net.HTTP;
import juglr.net.HTTPRequest;

import java.util.Map;

/**
 * An actor that stores messages in a Lucene index. The stored messages
 * <i>must</i> be of type {@link Box} and contain the mandatory fields:
 * <ul>
 *   <li>{@code _id} : An id for the message that is unique within the base</li>
 *   <li>{@code _rev} : The last known revision number for this id. Set to 0
 *                      on first insertion. Failing to specify the correct
 *                      revision number will result in a conflict and the whole
 *                      transaction being dropped</li>
 * </ul>
 * There is an additional optional field called {@code _index} which must
 * be of type {@code Box.Type.LIST} containing a list of field names which
 * will be indexed in the Lucene index.
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Jan 29, 2010
 */
public class WriterGatewayActor extends HTTPGatewayActor {

    public WriterGatewayActor() {

    }

    @Override
    public void react(Message message) {
        if (!(message instanceof HTTPRequest)) {
            replyTo(message, HTTP.Status.InternalError, "error",
                    "Expected HTTPRequest got '%s'",
                    message.getClass().getName());
            return;
        }

        HTTPRequest req = (HTTPRequest)message;
        Box body = req.getBody();

        // Make sure that responses from the QueryActor we forward to are
        // sent to the replyTo of 'message' and not this actor
        body.setReplyTo(message.getReplyTo());

        String baseName = getBase(req.getUri()).toString();
        Address writerAddress = findWriterActorForBase(baseName);


        Transaction transaction = new Transaction(baseName);
        transaction.setReplyTo(message.getReplyTo());
        try {
            for (Map.Entry<String,Box> entry : body.getMap().entrySet()) {
                Box box = entry.getValue();
                String id = entry.getKey();

                Box _id = box.get("_id");
                if (_id == null) {
                    replyTo(message, HTTP.Status.BadRequest,
                            "error", "No _id field for '%s'", id);
                    return;
                } else if (!id.equals(_id.getString())) {
                    replyTo(message, HTTP.Status.BadRequest,
                            "error", "Mismatching ids '%s' and '%s'",
                            id, box.getString("_id"));
                    return;
                }

                if (!box.has("_rev")) {
                    replyTo(message, HTTP.Status.BadRequest,
                            "error", "Box '%s' has no _rev field", id);
                    return;
                }

                if (box.has("_deleted")) {
                    transaction.delete(box);
                } else {
                    transaction.add(box);
                }
            }
        } catch (TransactionException e) {
            replyTo(message, HTTP.Status.InternalError,
                    "error", e.getMessage());
            return;
        } catch (Box.TypeException e) {
            replyTo(message, HTTP.Status.BadRequest,
                    "error", e.getMessage());
            return;
        } catch (MessageFormatException e) {
            replyTo(message, HTTP.Status.BadRequest,
                    "error", e.getMessage());
            return;
        }

        send(transaction, writerAddress);
    }

    private CharSequence getBase(CharSequence uri) {
        // We assume that 'uri' looks like '/$basename/query...'
        int uriLength = uri.length();
        int baseEnd = 1;
        for (; baseEnd < uriLength; baseEnd++) {
            if (uri.charAt(baseEnd) == '/') break;
        }

        return uri.subSequence(1, baseEnd);
    }

    private Address findWriterActorForBase(CharSequence baseName) {
        Address writerAddress = getBus().lookup(
                                          WriterActor.baseAddress(baseName));

        if (writerAddress == null) {
            writerAddress = new WriterActor(baseName.toString()).getAddress();
            getBus().start(writerAddress);
        }

        return writerAddress;
    }
}
