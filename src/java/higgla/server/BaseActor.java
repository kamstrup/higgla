package higgla.server;

import juglr.Actor;
import juglr.Box;
import juglr.Message;
import juglr.net.HTTP;
import juglr.net.HTTPResponse;

/**
 * Abstract base class for actors that handle operations for a given base.
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Feb 17, 2010
 */
public abstract class BaseActor extends Actor {

    protected String baseName;

    public BaseActor(String baseName) {
        this.baseName = baseName;
    }

    /**
     * Send a {@link HTTPResponse} to {@code request.getReplyTo()} containing
     * the field {@code field} with the value of {@code msg} formatted with the
     * {@code format} arguments as done with {@code String.format()}.
     * @param request the message to reply to
     * @param field the name of the field to set in the response
     * @param msg the value of the field in the response
     * @param format any format arguments required for the {@code msg} string
     */
    public void replyTo(Message request, HTTP.Status status,
                        String field, String msg, Object... format) {
        Box body = formatMessage(field, msg, format);
        Message resp = new HTTPResponse(status, body);
        send(resp, request.getReplyTo());
    }

    /**
     * Create a new Box of MAP type with the single field named {@code field}
     * set to the formatted string {@code msg},
     * @param field the name of the field to set
     * @param msg formatted string as passed to {@link String#format}
     * @param format format arguments used to escape values in {@code msg}
     * @return a newly allocated Box
     */
    public Box formatMessage(String field, String msg, Object... format) {
        Box box = Box.newMap();
        box.put(field, String.format(msg, format));
        return box;
    }
}
