package mydb.TupleDetail;

import mydb.Type;

import java.io.Serializable;
import java.util.Objects;

/**
 * A help class to facilitate organizing the information of each field
 * */
public class TupleCell implements Serializable {

    private static final long serialVersionUID = -8419977542514739836L;
    //field type
    Type fieldType;

    //field name
    String fieldName;

    public TupleCell(Type type, String name) {
        this.fieldName = name;
        this.fieldType = type;
    }

    @Override
    public String toString() {
        return fieldName + "(" + fieldType + ")";
    }

    @Override
    public boolean equals(Object o) {
        if(o == null)
            return false;

        //if two objects' hashcode() are equal.
        if(this == o) {
            return true;
        }
        if (o instanceof TupleCell) {
            TupleCell item = (TupleCell) o;
            return Objects.equals(fieldName, item.fieldName) && Objects.equals(fieldType, item.fieldType);
        } else return false;
    }

}
