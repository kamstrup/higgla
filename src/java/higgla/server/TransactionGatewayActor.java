package higgla.server;

import juglr.Actor;
import juglr.Message;
import static higgla.server.Storage.*;
import juglr.MessageFormatException;

/**
 * Responsible for commiting Transactions
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Feb 3, 2010
 */
public class TransactionGatewayActor extends Actor {

    /**
     * Commits a {@link Storage.Transaction} to the Lucene index
     * @param message
     */
    @Override
    public void react(Message message) {
        if (!(message instanceof Storage.Transaction)) {
            throw new MessageFormatException(
                    "Expected Storage.Transaction. Got "
                    + message.getClass().getName());
        }

        Storage.Transaction t = (Storage.Transaction)message;
    }
}
