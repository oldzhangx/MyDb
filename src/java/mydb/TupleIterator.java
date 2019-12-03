package mydb;

import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;
import java.util.Iterator;

public class TupleIterator implements DbIterator {
    private static final long serialVersionUID = -1020449676759920107L;

    private Iterator<Tuple> i = null;
    private TupleDetail tupleDetail = null;
    private Iterable<Tuple> tuples = null;

    public TupleIterator(TupleDetail tupleDetail, Iterable<Tuple> tuples) {
        this.tupleDetail = tupleDetail;
        this.tuples = tuples;

        for (Tuple t : tuples)
            if (!t.getTupleDetail().equals(tupleDetail))
                throw new IllegalArgumentException("incompatible tuple in tuple set");
    }

    public void open() {
        i = tuples.iterator();
    }

    public boolean hasNext() {
        return i.hasNext();
    }

    public Tuple next() {
        return i.next();
    }

    public void rewind() {
        close();
        open();
    }

    public TupleDetail getTupleDetail() {
        return tupleDetail;
    }

    public void close() {
        i = null;
    }
}
