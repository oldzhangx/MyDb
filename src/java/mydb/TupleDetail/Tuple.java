package mydb.TupleDetail;

import mydb.Field;
import mydb.RecordId;
import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

// tuple includes one tuple info, including tuple detail and tuple fields
public class Tuple implements Serializable {

    private static final long serialVersionUID = 5220765131088728433L;

    private TupleDetail tupleDetail;
    private RecordId recordId;
    private Field[] fields;

    public Iterator<Field> fields() {
        return new FieldsIterator();
    }
    private class FieldsIterator implements Iterator<Field> {
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

    public Tuple(TupleDetail td) {
        tupleDetail = td;
        fields = new Field[td.fieldNumber()];
    }

    public TupleDetail getTupleDetail() {
        return tupleDetail;
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public void setRecordId(RecordId redId) {
        recordId = redId;
    }

    public void setField(int index, Field field) {
        if (index < 0 || index > fields.length)  throw new IllegalArgumentException("field index out error");
        fields[index] = field;
    }

    public Field getField(int index) {
        if (index < 0 || index > fields.length)  throw new IllegalArgumentException("field index out error");
        return fields[index];
    }

    public String toString() {
        StringBuilder result = new StringBuilder();
        for (Field field : fields) {
            result.append(field).append(" ");
        }
        result.append("\n");
        return result.toString();
    }

}
