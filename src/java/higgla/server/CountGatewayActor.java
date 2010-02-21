package higgla.server;

import juglr.Address;
import juglr.Box;
import juglr.Message;
import juglr.net.HTTP;
import juglr.net.HTTPRequest;

/**
 * Responsible for forwarding HTTPRequests to the right CountActor
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Feb 19, 2010
 */
public class CountGatewayActor extends HTTPGatewayActor {

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

        // Make sure that responses from the CountActor we forward to are
        // sent to the replyTo of 'message' and not this actor
        body.setReplyTo(message.getReplyTo());

        Address countAddress = findCountActorForUri(req.getUri());
        send(body, countAddress);
    }

    private Address findCountActorForUri(CharSequence uri) {
        CharSequence base = extractBaseFromUri(uri);
        Address countAddress = getBus().lookup(CountActor.baseAddress(base));

        if (countAddress == null) {
            countAddress = new CountActor(base.toString()).getAddress();
            getBus().start(countAddress);
        }

        return countAddress;
    }

}
