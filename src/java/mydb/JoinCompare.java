package mydb;
import mydb.TupleDetail.Tuple;
import java.io.Serializable;

//Last Change: 11/23

// compares fields of two tuples
public class JoinCompare implements Serializable {

    private static final long serialVersionUID = -6792364188791199456L;

    // field1 The field index into the first tuple in the compare
    // field2 The field index into the second tuple in the compare
    private int field1, field2;
    private Comparison.Operation operation;
    public JoinCompare(int field1, Comparison.Operation operation, int field2) {
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
        if(t1==null && t2==null) return true;
        if ( t1 == null || t2 == null) return false;
            Field temp1 = t1.getField(field1);
            Field temp2 = t2.getField(field2);
            return temp1.compareWith(operation, temp2);
        }


    public int getField1()
    {
        return field1;
    }

    public int getField2()
    {
        return field2;
    }

    public Comparison.Operation getOperator()
    {
        return operation;
    }
}
