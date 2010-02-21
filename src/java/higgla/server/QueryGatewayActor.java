package higgla.server;

import juglr.Address;
import juglr.Box;
import juglr.Message;
import juglr.net.HTTP;
import juglr.net.HTTPRequest;

/**
 * Takes a {@link HTTPRequest} and routes it to the right {@link QueryActor} -
 * creating the relevant query actor if it is not registered on the bus.
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Feb 17, 2010
 */
public class QueryGatewayActor extends HTTPGatewayActor {
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

        Address queryAddress = findQueryActorForUri(req.getUri());
        send(body, queryAddress);
    }

    private Address findQueryActorForUri(CharSequence uri) {
        CharSequence base = extractBaseFromUri(uri);
        Address queryAddress = getBus().lookup(QueryActor.baseAddress(base));

        if (queryAddress == null) {
            queryAddress = new QueryActor(base.toString()).getAddress();
            getBus().start(queryAddress);
        }

        return queryAddress;
    }
}
