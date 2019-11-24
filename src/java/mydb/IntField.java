package mydb;

import java.io.*;

// Last Change: 11/23

/**
 * Instance of Field that stores a single integer.
 */
public class IntField implements Field {


    private static final long serialVersionUID = 4662232909262937129L;
    private int value;

    public int getValue() {
        return value;
    }

    /**
     * Constructor.
     *
     * @param i The value of this field.
     */
    public IntField(int i) {
        value = i;
    }

    public String toString() {
        return Integer.toString(value);
    }

    public int hashCode() {
        return value;
    }

    public boolean equals(Object field) {
        return ((IntField) field).value == value;
    }

    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeInt(value);
    }

    /**
     * Compare the specified field to the value of this Field.
     * Return semantics are as specified by Field.compare
     *
     * @throws IllegalArgumentException if val is not an IntField
     * @see Field#compareWith
     */
//    public boolean compareWith(Predicate.Op op, Field val) {
    public boolean compareWith(Predicate.Operation op, Field val) {

        IntField iVal = (IntField) val;

        switch (op) {
        case EQUALS:
        case LIKE:
            return value == iVal.value;
        case NOT_EQUALS:
            return value != iVal.value;

        case GREATER_THAN:
            return value > iVal.value;

        case GREATER_THAN_OR_EQ:
            return value >= iVal.value;

        case LESS_THAN:
            return value < iVal.value;

        case LESS_THAN_OR_EQ:
            return value <= iVal.value;
        }

        throw new IllegalArgumentException("int tuple compare error");
    }

    /**
     * Return the Type of this field.
     * @return Type.INT_TYPE
     */
	public Type getType() {
		return Type.INT_TYPE;
	}
}
