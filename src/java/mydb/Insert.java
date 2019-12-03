package mydb;

import mydb.Database.Database;
import mydb.Exception.DBException;
import mydb.Exception.TransactionAbortedException;
import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1496330920324262913L;

    // The transaction running the insert.
    TransactionId transactionId;
    // The child operator from which to read tuples to be inserted.
    DbIterator child;
    // The table in which to insert tuples.
    int tableId;
    // lines of insert tuples
    int insertCount;

    TupleDetail tupleDetail;

    boolean action;

    /**
     * Constructor.
     * 
     * @param transactionId
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DBException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId transactionId,DbIterator child, int tableId)
            throws DBException {
        this.transactionId=  transactionId;
        this.child  =child;
        this.tableId = tableId;
        insertCount = 0;
        tupleDetail = new TupleDetail(new Type[]{Type.INT_TYPE}, new String[]{null});
    }

    public TupleDetail getTupleDetail() {
        return tupleDetail;
    }

    public void open() throws DBException, TransactionAbortedException, IOException {
        super.open();
        child.open();

        while (child.hasNext()) {
            Tuple next = child.next();
            Database.getBufferPool().insertTuple(transactionId, tableId, next);
            insertCount++;
        }
        action = false;
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DBException, TransactionAbortedException {
        action = false;
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
    protected Tuple fetchNext() throws TransactionAbortedException, DBException {
        if (action) return null;

        Tuple insertedTuple = new Tuple(tupleDetail);
        insertedTuple.setField(0,new IntField(insertCount));
        action = true;
        return insertedTuple;
    }

    @Override
    public DbIterator[] getChildren() throws IOException, TransactionAbortedException, DBException {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    }
}
