package higgla.server;

import juglr.Actor;
import juglr.Box;
import juglr.Message;
import juglr.net.HTTP;
import juglr.net.HTTPResponse;

/**
 * Base class for actors used to forward {@link HTTPRequest}s  to
 * to actors handling only the body part of the HTTP request.
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Feb 17, 2010
 */
public abstract class HTTPGatewayActor extends Actor {

    /**
     * Format the field {@code field} in a new {@code Box} and send a
     * {@link HTTPResponse} to {@code request.getReplyTo()}
     * @param request the message to reply to
     * @param status the HTTP status code to set in the response
     * @param field the field name to set in the body Box
     * @param msg formatted message string to set as the value of {@code field}
     * @param format format arguments to the {@code msg} string
     */
    protected void replyTo(Message request, HTTP.Status status,
                           String field, String msg, Object... format) {
        Box body = Box.newMap();
        body.put(field, String.format(msg, format));
        HTTPResponse resp = new HTTPResponse(status, body);
        send(resp, request.getReplyTo());
    }

    protected CharSequence extractBaseFromUri(CharSequence uri) {
        // We assume that 'uri' looks like '/$basename/query...'
        int uriLength = uri.length();
        int baseEnd = 1;
        for (; baseEnd < uriLength; baseEnd++) {
            if (uri.charAt(baseEnd) == '/') break;
        }

        return uri.subSequence(1, baseEnd);
    }

}
