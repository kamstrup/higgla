package higgla.client;

import juglr.Box;

/**
 * Convenience representation of a query.
 */
public class Query {

    private Box box;

    /**
     * Create a new query targetting {@code base}
     * @param base
     */
    Query(String base) {
        box = Box.newMap();
        box.put("__base__", base);
        box.put("__query__", Box.newList());
    }

    public Query addTemplate(Box template) {
        template.checkType(Box.Type.MAP);
        box.get("__query__").add(template);
        return this;
    }

    public void setBase(String base) {
        box.put("__base__", base);
    }

    public String getBase() {
        return box.getString("__base__");
    }

    Box getRawQuery() {
        return box;
    }
}
