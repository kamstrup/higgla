package higgla.client;

/**
 * Exception throw when the Higgla server returns an error message
 */
public class HigglaException extends Exception {

    public HigglaException(String msg) {
        super(msg);
    }

}
