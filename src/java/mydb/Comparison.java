package mydb;
import mydb.TupleDetail.Tuple;
import java.io.Serializable;
//Last Change: 11/23


public class Comparison implements Serializable {

    private static final long serialVersionUID = -5302308970711948938L;
    int field;
    Operation operation;
    Field operand;

    public enum Operation implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        public static Operation getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i
         *            a valid integer Op index
         */
        public static Operation getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "like";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Constructor.
     *
     * @param field
     *            field number of passed in tuples to compare against.
     * @param operation
     *            operation to use for comparison
     * @param operand
     *            field value to compare passed in tuples to
     */
    public Comparison(int field, Operation operation, Field operand) {
        this.field = field;
        this.operation = operation;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField()
    {
        return field;
    }

    /**
     * @return the operator
     */
    public Operation getOperation()
    {
        return operation;
    }

    /**
     * @return the operand
     */
    public Field getOperand()
    {
        return operand;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     *
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        if (t == null) return false;
        //like the t > operand
        return t.getField(field).compareWith(operation, operand);
    }



    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string
     */
    public String toString() {

        return ""+"field = "+field+" op = "+ operation.toString()+" operand"+operand.toString();
    }
}