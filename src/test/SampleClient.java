import higgla.client.HigglaException;
import higgla.client.Query;
import higgla.client.Session;
import juglr.Box;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class SampleClient {

    public static void main(String[] args) throws Exception {
        Session session = new Session("localhost", 4567, "mybase");

        // Create a a document with the fields firstname and lastname indexed
        Box d1 = session.newDocument("mydoc1", 0, "firstname", "lastname");
        d1.put("firstname", "John");
        d1.put("lastname", "Doe");
        Box response = session.store(d1);
        System.out.println("Stored:");
        System.out.println(response);

        // Do a query
        Query q = session.prepareQuery();
        q.addTemplate(Box.newMap().put("firstname", "john"));

        System.out.println("Query results:");
        List<Box> result = session.sendQuery(q);
        for (Box box : result) {
            System.out.println(box.toString());
        }
    }
}
