package higgla.server;

import juglr.Address;
import juglr.Box;
import juglr.Message;
import juglr.net.HTTP;
import juglr.net.HTTPRequest;

/**
 * Takes a {@link HTTPRequest} and routes it to the right delegate.
 * If the request body is a MAP then it's passed to a {@link QueryActor} -
 * and if it's a LIST it's passed to a {@link GetActor}. In either case,
 * if the recipient actor isn't registered on the bus it will be created
 * automatically.
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

        Address delegate;
        if (body.getType() == Box.Type.MAP) {
             delegate = findQueryActorForUri(req.getUri());
        } else if (body.getType() == Box.Type.LIST) {
            delegate = findGetActorForUri(req.getUri());
        } else {
            replyTo(message, HTTP.Status.BadRequest,
                    "error", "Expected MAP or LIST. Found %s", body.getType());
            return;
        }
        send(body, delegate);
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

    private Address findGetActorForUri(CharSequence uri) {
        String base = extractBaseFromUri(uri).toString();
        Address getAddress = getBus().lookup(GetActor.baseAddress(base));

        if (getAddress == null) {
            getAddress = new GetActor(base).getAddress();
            getBus().start(getAddress);
        }

        return getAddress;
    }
}
