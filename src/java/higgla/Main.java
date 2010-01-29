package higgla;

import juglr.*;

/**
 * Main class for the Higgla application
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Jan 29, 2010
 */
public class Main {

    public static void main (String[] args) {
        // Make sure the default message bus is HTTPMessageBus
        System.setProperty("juglr.busclass", "juglr.net.HTTPMessageBus");

        // Set up all central actors
        Actor store = new StoreActor();
        Actor query = new QueryActor();

        // Register addresses
        try {
            MessageBus.getDefault().allocateNamedAddress(store, "store");
            MessageBus.getDefault().allocateNamedAddress(query, "query");
        } catch (AddressAlreadyOwnedException e) {
            System.err.println(
                    "Failed to allocate address on the message bus: "
                    + e.getMessage());
            System.exit(2);
        }

        // Start actors
        MessageBus.getDefault().start(store.getAddress());

        // Indefinite non-busy block
        synchronized (store) {
            try {                
                store.wait();
            } catch (InterruptedException e) {
                System.err.println("Interrupted");
                System.exit(1);
            }
        }
    }

}
