package higgla;

import juglr.Actor;
import juglr.Box;
import juglr.Message;
import juglr.MessageFormatException;

/**
 * Abstract helper class providing some functionality to handle and
 * validate messages.
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Jan 29, 2010
 */
public abstract class HigglaActor extends Actor {

    String[] mandatoryFields;

    public HigglaActor(String... mandatoryFields) {
        this.mandatoryFields = mandatoryFields;
    }

    /**
     * Throw a {@link MessageFormatException} if {@code msg} is not valid for
     * this actor and return the message casted to a {@link Box} in case it
     * was valid.
     * <p/>
     * Validation s performed by iterating over all {@code mandatoryFields}
     * passed to the constructor and checking that they are present.
     * @param msg the message to validate
     * @return {@code msg} casted as a {@link Box}
     * @throws MessageFormatException if the message is invalid
     */
    public Box validate(Message msg) throws MessageFormatException {
        if (!(msg instanceof Box))
            throw new MessageFormatException(String.format(
                    "Invalid message type %s expected %s",
                    msg.getClass().getName(), Box.class.getName()));

        Box box = (Box)msg;
        for (String field : mandatoryFields) {
            if (!box.has(field))
                throw missingField(field);
        }        

        Box index = box.get("__index__");
        if (index != null &&
            index.getType() != Box.Type.LIST) {
            throw new MessageFormatException(
                    "The __index__ field must contain a list, found " + index.getType());
        }

        return box;
    }

    private MessageFormatException missingField(String fieldName) {
        return new MessageFormatException(
               "Message does not contain contain mandatory field " + fieldName);
    }

    protected Box formatMsg(String field, String format, Object... args) {
        return Box.newMap().put(field, String.format(format, args));
    }
}
