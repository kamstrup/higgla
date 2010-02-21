package higgla.client;

import juglr.Box;

/**
 * Convenience representation of a query. Queries are created by calling
 * {@link higgla.client.Session#prepareQuery()}. When you have prepared a query
 * you submit it to the Higgla server by calling
 * {@link Session#sendQuery(Query)}.
 * <p/>
 * Higgla uses a template based query engine. A query consists of a collection
 * of templates and the query will match a document if the document matches any
 * one of the templates. Templates are added to the query by calling
 * {@link #addTemplate(juglr.Box)}.
 * <p/>
 * A document matches a template if each field in the template matces a free
 * text query on the same field in the document.
 */
public class Query {

    private Box box;
    private String name;

    /**
     * Create a new named query
     * @param queryName a name to identify this query by
     */
    Query(String queryName) {
        box = Box.newMap();
        box.put("_templates", Box.newList());
        name = queryName;
    }

    public Query addTemplate(Box template) {
        template.checkType(Box.Type.MAP);
        box.get("_templates").add(template);
        return this;
    }

    public Query setOffset(int offset) {
        box.put("_offset", offset);
        return this;
    }

    public Query setCount(int count) {
        box.put("_count", count);
        return this;
    }

    public String getName() {
        return name;
    }

    Box getRawQuery() {
        return box;
    }
}
