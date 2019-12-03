package mydb.Operation.Join;
import mydb.DbIterator;
import mydb.Exception.DBException;
import mydb.Exception.TransactionAbortedException;
import mydb.Operation.Join.Comparison;
import mydb.Operator;
import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;
import java.io.IOException;
import java.util.NoSuchElementException;


public class Filter extends Operator {

    private static final long serialVersionUID = 1644152417983932905L;

    // The compare to filter tuples with
    private Comparison m_pred;
    // The child operator
    private DbIterator m_it;

    public Filter(Comparison p, DbIterator child) {
        m_pred = p;
        m_it = child;
    }

    public Comparison getPredicate() {
        return m_pred;
    }

    public TupleDetail getTupleDetail() {
        return m_it.getTupleDetail();
    }

    public void open() throws DBException, NoSuchElementException,
            TransactionAbortedException, IOException {
        super.open();
        m_it.open();
    }

    public void close() {
        super.close();
        m_it.close();
    }

    public void rewind() throws DBException, TransactionAbortedException, IOException {
        m_it.rewind();
    }


    // find the next tuple that pass the filter
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DBException, IOException {
        while (m_it.hasNext())
        {
            Tuple tup = m_it.next();
            if (m_pred.filter(tup))
            {
                return tup;
            }
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {m_it};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        m_it = children[0];
    }

}