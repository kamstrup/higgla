package higgla.server;

import juglr.*;
import juglr.net.HTTP;
import juglr.net.HTTPServer;

import java.io.IOException;

import static java.lang.Integer.parseInt;

/**
 * Main class for the Higgla application
 *
 * @author Mikkel Kamstrup Erlandsen <mailto:mke@statsbiblioteket.dk>
 * @since Jan 29, 2010
 */
public class Main {

    public static void main (String[] args) {
        int port = 4567;
        if (args.length >= 1) {
            try {
                port = parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println(String.format(
                        "Port not a number '%s': %s. Bailing out.",
                        args[0], e.getMessage()));
                System.exit(1);
            }
        }

        // Set up all central actors
        Actor store = new StoreActor();
        Actor query = new QueryGatewayActor();
        Actor get = new GetActor();

        // Start actors
        MessageBus.getDefault().start(store.getAddress());
        MessageBus.getDefault().start(query.getAddress());
        MessageBus.getDefault().start(get.getAddress());

        // Set up the HTTP server
        HTTPServer server = null;
        try {
             server = new HTTPServer(port);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Failed to create HTTP server. Bailing out.");
            System.exit(2);
        }

        server.registerHandler(
                    "^/[^/]+/query/?$", query.getAddress(), HTTP.Method.GET);
                    //".*", query.getAddress(), HTTP.Method.GET);
        //server.registerHandler(
        //            "^/[^/]+/search\\?.+$", search.getAddress(), HTTP.Method.GET);
        //server.registerHandler(
        //            "^/[^/]+/changes\\?.+$", changes.getAddress(), HTTP.Method.GET);
        //server.registerHandler(
        //            "^/[^/]+/count/?$", count.getAddress(), HTTP.Method.GET);
        //server.registerHandler(
        //            "^/[^/]+/changes\\?.+$", changes.getAddress(), HTTP.Method.GET);
        //server.registerHandler(
        //            "^/[^/]+/[^/]+$", get.getAddress(), HTTP.Method.GET);
        //server.registerHandler(
        //            "^/[^/]+/[^/]+$", put.getAddress(), HTTP.Method.PUT);

        server.start();

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
