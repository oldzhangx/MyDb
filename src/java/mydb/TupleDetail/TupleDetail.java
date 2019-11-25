package mydb.TupleDetail;

import mydb.Type;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

// fields are in one tuple
public class TupleDetail implements Serializable {

    private static final long serialVersionUID = 7041402579811379073L;

    //length of tuplecells
//    private Integer fieldNumber;

    private TupleCell[] tupleCells;

    public Iterator<TupleCell> iterator() {
        return new TDItemIterator();
    }

    private class TDItemIterator implements Iterator<TupleCell> {

        //max.pos +1 = length
        int pos = 0;

        @Override
        public boolean hasNext() {
            return tupleCells.length > pos;
        }

        @Override
        public TupleCell next() {
            if(hasNext()) return tupleCells[pos++];
            throw  new NoSuchElementException();
        }
    }

    public TupleDetail(TupleCell[] tupleCells) {
        if(tupleCells == null) throw new IllegalArgumentException("tdItems null error");
        if(tupleCells.length == 0) throw new IllegalArgumentException("tdItems length 0 tuple error");
        this.tupleCells = tupleCells;

    }
    public TupleDetail(Type[] typeAr) {
        this(typeAr, new String[typeAr.length]);
    }


    public TupleDetail(Type[] typeAr, String[] fieldAr) {
        if(typeAr == null) throw new IllegalArgumentException("typeAr null error");
        if(typeAr.length == 0) throw new IllegalArgumentException("typeAr length 0 tuple error");
        if(typeAr.length != fieldAr.length) throw new IllegalArgumentException("typeAr length has to be equal to fieldAr");

        tupleCells = new TupleCell[typeAr.length];

        for(int i = 0; i < typeAr.length; i++) {
            tupleCells[i] = new TupleCell(typeAr[i], fieldAr[i]);
        }
    }

    public int fieldNumber() {
        return tupleCells.length;
    }

    public String getFieldName(int i) throws NoSuchElementException {
        if(tupleCells == null) throw new NoSuchElementException();
        if (i < 0 || i> tupleCells.length) throw new NoSuchElementException();
        return tupleCells[i].fieldName;
    }


    public Type getFieldType(int i) throws NoSuchElementException {
        if(tupleCells == null) throw new NoSuchElementException();
        if (i < 0 || i> this.tupleCells.length) throw new NoSuchElementException();
        return tupleCells[i].fieldType;
    }

    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if(name == null) throw new NoSuchElementException();
        for(int i = 0; i< tupleCells.length; i++){
            if (name.equals(tupleCells[i].fieldName))
                return i;
        }
        throw new NoSuchElementException();
    }

    public int getSize() {
        int totalSize = 0;
        for (TupleCell tupleCell : tupleCells) {
            totalSize = totalSize + tupleCell.fieldType.getLen();
        }
        return totalSize;
    }

    public static TupleDetail merge(TupleDetail td1, TupleDetail td2) {
        if(td1 == null) return td2;
        if(td2 == null) return td1;
        TupleCell[] tupleCells = new TupleCell[td1.tupleCells.length+ td2.tupleCells.length];
        System.arraycopy(td1.tupleCells, 0, tupleCells, 0, td1.tupleCells.length);
        System.arraycopy(td2.tupleCells, 0, tupleCells, td1.tupleCells.length, td2.tupleCells.length);
        return new TupleDetail(tupleCells);
    }


    public boolean equals(Object o) {
        if(o == null) return false;
        if(o == this) return true;
        if(o.getClass() != this.getClass()) return false;
        TupleDetail tupleDetail = (TupleDetail) o;
        if(tupleDetail.tupleCells.length != tupleCells.length) return false;
        for(int i = 0; i< tupleCells.length ; i++)
            if(!tupleCells[i].equals(tupleDetail.tupleCells[i]))
                return false;
            return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    public String toString() {
        TupleCell[] tupleCells = this.tupleCells;
        StringBuilder result = new StringBuilder();
        for(TupleCell tupleCell : tupleCells)
            result.append(" ").append(tupleCell.toString());
        return result.toString();
    }
}
