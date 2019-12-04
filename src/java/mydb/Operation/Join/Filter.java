package mydb.Operation.Join;
import mydb.DbIterator;
import mydb.Exception.DBException;
import mydb.Exception.TransactionAbortedException;
import mydb.Operator;
import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;
import java.io.IOException;
import java.util.NoSuchElementException;


public class Filter extends Operator {

    private static final long serialVersionUID = 1644152417983932905L;

    // The compare to filter tuples with
    private Comparison comparison;
    // The child operator
    private DbIterator iterator;

    public Filter(Comparison p, DbIterator child) {
        comparison = p;
        iterator = child;
    }

    public Comparison getPredicate() {
        return comparison;
    }

    public TupleDetail getTupleDetail() {
        return iterator.getTupleDetail();
    }

    public void open() throws DBException, NoSuchElementException,
            TransactionAbortedException, IOException {
        super.open();
        iterator.open();
    }

    public void close() {
        super.close();
        iterator.close();
    }

    public void rewind() throws DBException, TransactionAbortedException, IOException {
        iterator.rewind();
    }


    // find the next tuple that pass the filter
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DBException, IOException {
        while (iterator.hasNext())
        {
            Tuple tup = iterator.next();
            if (comparison.filter(tup))
            {
                return tup;
            }
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {iterator};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        iterator = children[0];
    }

}