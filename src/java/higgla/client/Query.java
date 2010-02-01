package higgla.client;

import juglr.Box;

/**
 * Convenience representation of a query. Queries are created by calling
 * {@link higgla.client.Session#prepareQuery()}.
 * <p/>
 * Higgla uses a template based query engine. A query consists of a collection
 * of templates and the query will match a document if the document matches any
 * one of templates. Templates are added to the query by calling
 * {@link #addTemplate(juglr.Box)}.
 * <p/>
 * A document matches a template if each field in the template matces a free
 * text query on the same field in the document.
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
