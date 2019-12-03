package mydb;

import mydb.Database.Database;
import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = -2713633299969413734L;

    TransactionId transactionId;
    DbIterator child;
    TupleDetail tupleDetail;

    int deleteCount;

    boolean action;


    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param transactionId
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId transactionId, DbIterator child) {
        this.transactionId = transactionId;
        this.child = child;
        tupleDetail = new TupleDetail(new Type[]{Type.INT_TYPE}, new String[]{null});
        deleteCount = 0;
    }

    public TupleDetail getTupleDetail() {
        return tupleDetail;
    }

    public void open() throws DbException, TransactionAbortedException, IOException {
        child.open();
        super.open();
        while (child.hasNext()) {
            Tuple next = child.next();
            Database.getBufferPool().deleteTuple(transactionId, next);
            deleteCount++;
        }
        action = false;
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        action = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    // TODO: why need action
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (action) return null;

        Tuple deleted_num=new Tuple(tupleDetail);
        deleted_num.setField(0,new IntField(deleteCount));
        action = true;
        return deleted_num;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    }

}
