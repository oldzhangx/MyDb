package mydb.TupleDetail;

import mydb.Field;
import mydb.RecordId;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 5220765131088728433L;

    private TupleDetail tupleDetail;
    private RecordId recordId;
    private Field[] fields;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDetail td) {
        tupleDetail = td;
        fields = new Field[td.fieldNumber()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDetail getTupleDesc() {
        return tupleDetail;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param redId
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId redId) {
        recordId = redId;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param index
     *            index of the field to change. It must be a valid index.
     * @param field
     *            new value for the field.
     */
    public void setField(int index, Field field) {
        if (index < 0 || index > fields.length)  throw new IllegalArgumentException("field index out error");
        fields[index] = field;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param index
     *            field index to return. Must be a valid index.
     */
    public Field getField(int index) {
        if (index < 0 || index > fields.length)  throw new IllegalArgumentException("field index out error");
        return fields[index];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (Field field : fields) {
            result.append(field).append(" ");
        }
        result.append("\n");
        return result.toString();
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
        return new FieldRow();
    }

    private class FieldRow implements Iterator<Field> {
        private int num = 0;

        @Override
        public boolean hasNext() {
            return num < fields.length ;
        }

        @Override
        public Field next() {
            if (!hasNext())  throw new NoSuchElementException();
            return fields[num++];
        }
    }
}
