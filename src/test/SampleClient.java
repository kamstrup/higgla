import higgla.client.HigglaException;
import higgla.client.Query;
import higgla.client.Session;
import juglr.Box;

import java.io.IOException;

/**
 *
 */
public class SampleClient {

    public static void main(String[] args) throws Exception {
        Session session = new Session("localhost", 4567, "mybase");
        Query q = session.prepareQuery();
        q.addTemplate(Box.newMap().put("flash", "extra"));

        session.sendQuery(q);        
    }
}
