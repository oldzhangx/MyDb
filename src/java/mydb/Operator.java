package mydb;
import mydb.Exception.DBException;
import mydb.Exception.TransactionAbortedException;
import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;

import java.io.IOException;
import java.util.NoSuchElementException;

//Last Change: 11/23


public abstract class Operator implements DbIterator {

    private static final long serialVersionUID = -8386431487731922691L;

    public boolean hasNext() throws DBException, TransactionAbortedException, IOException {
        if (!this.open)
            throw new IllegalStateException("Operator not yet open");
        if (next == null)
            next = fetchNext();
        return next != null;
    }

    public Tuple next() throws DBException, TransactionAbortedException,
            NoSuchElementException, IOException {
        if (next == null) {
            next = fetchNext();
            if (next == null)
                throw new NoSuchElementException();
        }

        Tuple result = next;
        next = null;
        return result;
    }

    //Returns the next Tuple in the iterator
    protected abstract Tuple fetchNext() throws DBException,
            TransactionAbortedException, IOException;


    public void close() {
        // Ensures that a future call to next() will fail
        next = null;
        this.open = false;
    }

    private Tuple next = null;
    private boolean open = false;
    private int estimatedCardinality = 0;

    public void open() throws DBException, TransactionAbortedException, IOException {
        this.open = true;
    }


    // return the children DbIterators of this operator
    public abstract DbIterator[] getChildren() throws IOException, TransactionAbortedException, DBException;

    // children[0] and children[1]
    public abstract void setChildren(DbIterator[] children);


    public abstract TupleDetail getTupleDetail();


    // The estimated cardinality of this operator.
    public int getEstimatedCardinality() {
        return this.estimatedCardinality;
    }


    protected void setEstimatedCardinality(int card) {
        this.estimatedCardinality = card;
    }

}