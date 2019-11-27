package mydb;

import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1496330920324262913L;

    //The transaction running the insert.
    TransactionId transactionId;
    //The child operator from which to read tuples to be inserted.
    DbIterator child;
    //The table in which to insert tuples.
    int tableId;

    /**
     * Constructor.
     * 
     * @param transactionId
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId transactionId,DbIterator child, int tableId)
            throws DbException {
        this.transactionId=  transactionId;
        this.child  =child;
        this.tableId = tableId;

    }

    public TupleDetail getTupleDetail() {
        return child.getTupleDetail();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    }

    public void close() {
        // some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return null;
    }

    @Override
    public DbIterator[] getChildren() throws IOException, TransactionAbortedException, DbException {
        DbIterator[] result = new DbIterator[getTupleDetail().getSize()];
        if(hasNext())
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    }
}
