package mydb.TupleDetail;

import mydb.Type;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

// fields are in one tuple
public class TupleDetail implements Serializable {

    private static final long serialVersionUID = 7041402579811379073L;

    //field size/number
    private Integer fieldNumber;

    private TupleCell[] tupleCells;

    public Iterator<TupleCell> iterator() {
        return new TDItemIterator();
    }

    private class TDItemIterator implements Iterator<TupleCell> {

        int pos = 0;

        @Override
        public boolean hasNext() {
            return tupleCells.length > pos;
        }

        @Override
        public TupleCell next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return tupleCells[pos++];
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDetail(Type[] typeAr) {
        this(typeAr, new String[typeAr.length]);
    }

    public TupleDetail(TupleCell[] tupleCells) {
        if(tupleCells == null) throw new IllegalArgumentException("tdItems null error");
        if(tupleCells.length == 0) throw new IllegalArgumentException("tdItems length 0 tuple error");

        fieldNumber = tupleCells.length;
        this.tupleCells = tupleCells;
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDetail(Type[] typeAr, String[] fieldAr) {
        if(typeAr == null) throw new IllegalArgumentException("typeAr null error");
        if(typeAr.length == 0) throw new IllegalArgumentException("typeAr length 0 tuple error");
        if(typeAr.length != fieldAr.length) throw new IllegalArgumentException("typeAr length has to be equal to fieldAr");

        fieldNumber = typeAr.length;
        tupleCells = new TupleCell[fieldNumber];

        for(int i = 0; i < fieldNumber; i++) {
            tupleCells[i] = new TupleCell(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int fieldNumber() {
        return this.fieldNumber;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i> fieldNumber) throw new NoSuchElementException();
        return tupleCells[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i> this.fieldNumber) throw new NoSuchElementException();
        return tupleCells[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if(name == null) throw new NoSuchElementException();
        for(int i = 0; i< tupleCells.length; i++){
            if (name.equals(tupleCells[i].fieldName))
                return i;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int totalSize = 0;
        for (TupleCell tupleCell : tupleCells) {
            totalSize = totalSize + tupleCell.fieldType.getLen();
        }
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDetail merge(TupleDetail td1, TupleDetail td2) {
        if(td1 == null) return td2;
        if(td2 == null) return td1;
        TupleCell[] tupleCells = new TupleCell[td1.tupleCells.length+ td2.tupleCells.length];
        for(int i = 0; i< td1.tupleCells.length; i++){
            tupleCells[i] = td1.tupleCells[i];
        }
        for(int i = 0; i< td2.tupleCells.length; i++){
            tupleCells[i+ td1.tupleCells.length] = td2.tupleCells[i];
        }
        return new TupleDetail(tupleCells);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if(o == null) return false;
        if(o == this) return true;
        if(o.getClass() == this.getClass()){
            TupleDetail tupleDetail = (TupleDetail) o;
            if (!tupleDetail.fieldNumber.equals(this.fieldNumber)) return false;
            for(int i = 0; i< tupleDetail.fieldNumber; i++)
                if(!tupleDetail.tupleCells[i].equals(this.tupleCells[i]))
                    return false;
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        TupleCell[] tupleCells = this.tupleCells;
        StringBuilder result = new StringBuilder();
        for(TupleCell tupleCell : tupleCells)
            result.append(" ").append(tupleCell.toString());
        return result.toString();
    }
}
