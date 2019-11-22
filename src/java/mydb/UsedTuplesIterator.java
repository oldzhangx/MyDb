package mydb;

import mydb.TupleDetail.Tuple;

import java.util.Iterator;

class UsedTuplesIterator implements Iterator<Tuple> {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Tuple next() {
        return null;
    }
}
