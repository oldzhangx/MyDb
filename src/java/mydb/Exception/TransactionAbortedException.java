package mydb.Exception;

import java.lang.Exception;

// when a transaction has aborted
public class TransactionAbortedException extends Exception {

    private static final long serialVersionUID = 5826891078988919640L;

    public TransactionAbortedException() {super("there is one transaction has aborted");}
}
