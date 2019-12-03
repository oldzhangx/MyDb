package mydb.TupleDetail;

import mydb.Type;
import java.io.Serializable;
import java.util.Objects;

// define name and type
public class TupleCell implements Serializable {

    private static final long serialVersionUID = -8419977542514739836L;

    String fieldName;
    Type fieldType;

    public TupleCell(Type type, String name) {
        this.fieldName = name;
        this.fieldType = type;
    }

    @Override
    public String toString() {
        return fieldName + " (" + fieldType.name() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if(o == null)
            return false;

        //if two objects' hashcode() are equal.
        if(this == o) {
            return true;
        }
        if(o.getClass() != this.getClass()) return false;
        TupleCell item = (TupleCell) o;
        return Objects.equals(fieldName, item.fieldName) && Objects.equals(fieldType, item.fieldType);
    }

}
