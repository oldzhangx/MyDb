package mydb;

import java.io.IOException;

/**
 * Transaction encapsulates information about the state of
 * a transaction and manages transaction commit / abort.
 */

public class Transaction {
    private final TransactionId transactionId;
    volatile boolean started = false;

    public Transaction() {
        transactionId = new TransactionId();
    }

    /** Start the transaction running */
    public void start() throws IOException {
        started = true;
        Database.getLogFile().logXactionBegin(transactionId);
    }

    public TransactionId getId() {
        return transactionId;
    }

    /** Finish the transaction */
    public void commit() throws IOException {
        transactionComplete(false);
    }

    /** Finish the transaction */
    public void abort() throws IOException {
        transactionComplete(true);
    }

    /** Handle the details of transaction commit / abort */
    public void transactionComplete(boolean abort) throws IOException {
        if(!started)
            return;
        if(abort) Database.getLogFile().logAbort(transactionId); //does rollback too
        else{
            Database.getBufferPool().flushPages(transactionId);
            Database.getLogFile().logCommit(transactionId);
        }
        Database.getBufferPool().transactionComplete(transactionId, !abort); // release locks
        started = false;
    }

}
