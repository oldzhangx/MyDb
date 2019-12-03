package mydb;

import mydb.Exception.DBException;
import mydb.Exception.TransactionAbortedException;
import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;

import java.io.IOException;
import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * Query is a wrapper class to manage the execution of queries. It takes a query
 * plan in the form of a high level DbIterator (built by initiating the
 * constructors of query plans) and runs it as a part of a specified
 * transaction.
 * 
 * @author Sam Madden
 */

public class Query implements Serializable {


    private static final long serialVersionUID = 6016845657122104041L;
    transient private DbIterator op;
    transient private LogicalPlan logicalPlan;
    TransactionId tid;
    transient private boolean started = false;

    public TransactionId getTransactionId() {
        return this.tid;
    }

    public void setLogicalPlan(LogicalPlan lp) {
        this.logicalPlan = lp;
    }

    public LogicalPlan getLogicalPlan() {
        return this.logicalPlan;
    }

    public void setPhysicalPlan(DbIterator pp) {
        this.op = pp;
    }

    public DbIterator getPhysicalPlan() {
        return this.op;
    }

    public Query(TransactionId t) {
        tid = t;
    }

    public Query(DbIterator root, TransactionId t) {
        op = root;
        tid = t;
    }

    public void start() throws DBException,
            TransactionAbortedException, IOException {
        op.open();

        started = true;
    }

    public TupleDetail getOutputTupleDesc() {
        return this.op.getTupleDetail();
    }

    /** @return true if there are more tuples remaining. */
    public boolean hasNext() throws DBException, TransactionAbortedException, IOException {
        return op.hasNext();
    }

    /**
     * Returns the next tuple, or throws NoSuchElementException if the iterator
     * is closed.
     * 
     * @return The next tuple in the iterator
     * @throws DBException
     *             If there is an error in the database system
     * @throws NoSuchElementException
     *             If the iterator has finished iterating
     * @throws TransactionAbortedException
     *             If the transaction is aborted (e.g., due to a deadlock)
     */
    public Tuple next() throws DBException, NoSuchElementException,
            TransactionAbortedException, IOException {
        if (!started)
            throw new DBException("Database not started.");

        return op.next();
    }

    /** Close the iterator */
    public void close() throws IOException {
        op.close();
        started = false;
    }

    public void execute() throws IOException, DBException, TransactionAbortedException {
        TupleDetail td = this.getOutputTupleDesc();

        StringBuilder names = new StringBuilder();
        for (int i = 0; i < td.fieldNumber(); i++) {
            names.append(td.getFieldName(i)).append("\t");
        }
        System.out.println(names);
        for (int i = 0; i < names.length() + td.fieldNumber() * 4; i++) {
            System.out.print("-");
        }
        System.out.println();

        this.start();
        int cnt = 0;
        while (this.hasNext()) {
            Tuple tup = this.next();
            System.out.println(tup);
            cnt++;
        }
        System.out.println("\n " + cnt + " rows.");
        this.close();
    }
}
