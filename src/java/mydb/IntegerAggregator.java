package mydb;

import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */

// COUNT，  SUM，  AVG，  MIN，  MAX
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 587316638506548325L;

    //the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
    int groupByFieldIndex = NO_GROUPING;

    //the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
    Type groupByFieldType = null;

    //the 0-based index of the aggregate field in the tuple
    int aggregateFieldIndex;

    //the aggregation operator
    Opertion opertion;

    HashMap<Field, Integer> countResult ;

    TupleDetail tupleDetail;


    public IntegerAggregator(int groupByFieldIndex, Type groupByFieldType, int aggregateFieldIndex, Opertion what) {
        this.groupByFieldIndex = groupByFieldIndex;
        this.groupByFieldType = groupByFieldType;
        this.aggregateFieldIndex = aggregateFieldIndex;
        this.opertion = what;
        countResult = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {

    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        throw new
        UnsupportedOperationException("please implement me for proj2");
    }

}
