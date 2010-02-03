package higgla.server;

/**
 * 
 */
public class TransactionException extends Exception {

    public TransactionException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public TransactionException(String msg) {
        super(msg);
    }

}
