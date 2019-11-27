package mydb;

import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * Project is an operator that implements a relational projection.
 */
public class Project extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private TupleDetail td;
    private ArrayList<Integer> outFieldIds;

    /**
     * Constructor accepts a child operator to read tuples to apply projection
     * to and a list of fields in output tuple
     * 
     * @param fieldList
     *            The ids of the fields child's tupleDesc to project out
     * @param typesList
     *            the types of the fields in the final projection
     * @param child
     *            The child operator
     */
    public Project(ArrayList<Integer> fieldList, ArrayList<Type> typesList,
            DbIterator child) {
        this(fieldList,typesList.toArray(new Type[]{}),child);
    }
    
    public Project(ArrayList<Integer> fieldList, Type[] types,
            DbIterator child) {
        this.child = child;
        outFieldIds = fieldList;
        String[] fieldAr = new String[fieldList.size()];
        TupleDetail childtd = child.getTupleDetail();

        for (int i = 0; i < fieldAr.length; i++) {
            fieldAr[i] = childtd.getFieldName(fieldList.get(i));
        }
        td = new TupleDetail(types, fieldAr);
    }

    public TupleDetail getTupleDetail() {
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException, IOException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException, IOException {
        child.rewind();
    }

    /**
     * Operator.fetchNext implementation. Iterates over tuples from the child
     * operator, projecting out the fields from the tuple
     * 
     * @return The next tuple, or null if there are no more tuples
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException, IOException {
        while (child.hasNext()) {
            Tuple t = child.next();
            Tuple newTuple = new Tuple(td);
            newTuple.setRecordId(t.getRecordId());
            for (int i = 0; i < td.fieldNumber(); i++) {
                newTuple.setField(i, t.getField(outFieldIds.get(i)));
            }
            return newTuple;
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
	if (this.child!=children[0])
	{
	    this.child = children[0];
	}
    }
    
}
