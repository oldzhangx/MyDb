
package mydb;
import mydb.TupleDetail.Tuple;
import java.io.Serializable;

//Last Change: 11/23



/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {


    private static final long serialVersionUID = -6792364188791199456L;
    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     *
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    private int field1, field2;
    private Predicate.Operation operation;
    public JoinPredicate(int field1, Predicate.Operation operation, int field2) {
        // some code goes here
        this.field1=field1;
        this.field2=field2;
        this.operation = operation;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     *
     * @return true if the tuples satisfy the predicate.
     */

    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        if (t1 == null || t2 == null) return false;
            Field temp1 = t1.getField(field1);
            Field temp2 = t2.getField(field2);
            return temp1.compareWith(operation, temp2);
        }


    public int getField1()
    {
        // some code goes here
        return field1;
    }

    public int getField2()
    {
        // some code goes here
        return field2;
    }

    public Predicate.Operation getOperator()
    {
        // some code goes here
        return operation;
    }
}
