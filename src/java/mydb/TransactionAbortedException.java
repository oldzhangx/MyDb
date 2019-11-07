package mydb;

import java.lang.Exception;

/** Exception that is thrown when a transaction has aborted. */
public class TransactionAbortedException extends Exception {


    private static final long serialVersionUID = 5826891078988919640L;

    public TransactionAbortedException() {
    }
}
