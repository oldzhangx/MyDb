package mydb;
import mydb.Exception.DBException;
import mydb.Exception.TransactionAbortedException;
import mydb.TupleDetail.Tuple;

import java.io.IOException;
import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * DbFileIterator is the iterator interface that all SimpleDB Dbfile should
 * implement.
 */
public interface DbFileIterator extends Serializable {
    /**
     * Opens the iterator
     * @throws DBException when there are problems opening/accessing the database.
     */
    public void open()
            throws DBException, TransactionAbortedException, IOException;

    /** @return true if there are more tuples available. */
    public boolean hasNext()
            throws DBException, TransactionAbortedException, IOException;

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next()
            throws DBException, TransactionAbortedException, NoSuchElementException, IOException;

    /**
     * Resets the iterator to the start.
     * @throws DBException When rewind is unsupported.
     */
    public void rewind() throws DBException, TransactionAbortedException, IOException;

    /**
     * Closes the iterator.
     */
    public void close();
}
