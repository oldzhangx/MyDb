package mydb;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Instance of Field that stores a single integer.
 */
public class LongField implements Field {

	private static final long serialVersionUID = 1L;

	private long value;

    public long getValue() {
        return value;
    }

    /**
     * Constructor.
     *
     * @param i The value of this field.
     */
    public LongField(long i) {
        value = i;
    }

    public String toString() {
        return Long.toString(value);
    }

    public int hashCode() {
        return Long.hashCode(value);
    }

    public boolean equals(Object field) {
        return ((LongField) field).value == value;
    }

    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeLong(value);
    }

    /**
     * Compare the specified field to the value of this Field.
     * Return semantics are as specified by Field.compare
     *
     * @throws IllegalCastException if val is not an IntField
     * @see Field#compare
     */
    public boolean compare(Predicate.Operation op, Field val) {

        LongField iVal = (LongField) val;

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

        return false;
    }

    /**
     * Return the Type of this field.
     * @return Type.INT_TYPE
     */
	public Type getType() {
		return Type.LONG_TYPE;
	}
}
