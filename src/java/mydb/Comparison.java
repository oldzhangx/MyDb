package mydb;
import mydb.TupleDetail.Tuple;
import java.io.Serializable;
//Last Change: 11/23

// store the result of ().getField(index) op operand ?
public class Comparison implements Serializable {

    private static final long serialVersionUID = -5302308970711948938L;
    // field number of passed in tuples to compare against.
    int field;
    // operation to use for comparison
    Operation operation;
    //field value to compare passed in tuples to
    Field operand;

    public enum Operation implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

//        public static Operation getOp(String s) {
//            return getOp(Integer.parseInt(s));
//        }
//
//        public static Operation getOp(int i) {
//            return values()[i];
//        }

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
                return "!=";
            throw new IllegalStateException("impossible to reach here");
        }
    }


    public Comparison(int field, Operation operation, Field operand) {
        this.field = field;
        this.operation = operation;
        this.operand = operand;
    }

    public int getField()
    {
        return field;
    }

    public Operation getOperation()
    {
        return operation;
    }

    public Field getOperand()
    {
        return operand;
    }

    public boolean filter(Tuple t) {
        if (t == null) return false;
        //like the t > operand
        return t.getField(field).compareWith(operation, operand);
    }

    public String toString() {

        return ""+"field = "+field+" op = "+ operation.toString()+" operand"+operand.toString();
    }
}