package higgla.client;

/**
 * Exception throw when the Higgla server returns an error message or the Higgla
 * server returns an invalid response
 */
public class HigglaException extends Exception {

    public HigglaException(String msg) {
        super(msg);
    }

}
