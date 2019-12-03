package mydb;
import mydb.Exception.DBException;
import mydb.Exception.TransactionAbortedException;
import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;
import java.io.IOException;
import java.util.NoSuchElementException;


public class Filter extends Operator {

    private static final long serialVersionUID = 1644152417983932905L;

    private Comparison m_pred;
    private DbIterator m_it;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
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

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Comparison#filter
     */
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