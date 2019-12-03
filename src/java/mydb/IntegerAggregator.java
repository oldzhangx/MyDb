package mydb;

import mydb.Operation.Aggregator;
import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;
import mydb.TupleDetail.TupleIterator;

import java.util.ArrayList;
import java.util.HashMap;

import static mydb.Operation.Aggregator.Opertion.*;
import static mydb.Type.INT_TYPE;


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

    HashMap<Field, Integer> sumResult ;

    TupleDetail tupleDetail;


    public IntegerAggregator(int groupByFieldIndex, Type groupByFieldType, int aggregateFieldIndex, Opertion what) {
        this.groupByFieldIndex = groupByFieldIndex;
        this.groupByFieldType = groupByFieldType;
        this.aggregateFieldIndex = aggregateFieldIndex;
        this.opertion = what;
        countResult = new HashMap<>();
        sumResult = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {

        if(tup == null ) throw new IllegalArgumentException("tup null error");
        int length = tup.getTupleDetail().fieldNumber();
        if(aggregateFieldIndex >= length || aggregateFieldIndex < 0)
            throw new IllegalArgumentException("aggregateFieldIndex out of field index");
        tupleDetail = tup.getTupleDetail();

        Field aggregateField = tup.getField(aggregateFieldIndex);
        if (aggregateField.getType() != INT_TYPE) throw new IllegalArgumentException("aggregateField type isn't integer_type");
        int aggregateFieldValue = ((IntField)aggregateField).getValue();

        Field groupByField = groupByFieldIndex== NO_GROUPING? null: tup.getField(groupByFieldIndex);

        if( (groupByField==null && groupByFieldType== null ) ||
                (groupByField!= null && groupByFieldType!=null && groupByField.getType().equals(groupByFieldType)) ){

            if(opertion == MIN){
                int min = countResult.getOrDefault(groupByField,Integer.MAX_VALUE);
                if(aggregateFieldValue< min) countResult.put(groupByField,aggregateFieldValue);
            }else if(opertion == MAX){
                int max = countResult.getOrDefault(groupByField,Integer.MIN_VALUE);
                if(aggregateFieldValue> max) countResult.put(groupByField,aggregateFieldValue);
            }else if(opertion == SUM || opertion == AVG || opertion == COUNT){
                int sum = sumResult.getOrDefault(groupByField,0);
                sumResult.put(groupByField,aggregateFieldValue + sum);
                int count = countResult.getOrDefault(groupByField,0);
                countResult.put(groupByField,1+count);
            }else throw new IllegalArgumentException("operation type not right");

        }else throw new IllegalArgumentException("groupByFieldType not equal");
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
        Type[] fieldType = groupByFieldType==null? new Type[]{INT_TYPE}:
                new Type[]{groupByFieldType, INT_TYPE};

        String[] fieldName = groupByFieldType==null? new String[]{tupleDetail.getFieldName(aggregateFieldIndex)}:
                new String[]{tupleDetail.getFieldName(groupByFieldIndex), tupleDetail.getFieldName(aggregateFieldIndex)};

        TupleDetail tupleDetail = new TupleDetail(fieldType, fieldName);

        ArrayList<Tuple> tuples = new ArrayList<>();

        for(Field field: countResult.keySet()){
            Tuple tuple = new Tuple(tupleDetail);
            double result;
            if(opertion == SUM)
                result = sumResult.get(field);
            else if(opertion == AVG)
                result = (double)sumResult.get(field)/countResult.get(field);
            else result = countResult.get(field);

            if(field == null) tuple.setField(0, new IntField((int)result));
            else {
                tuple.setField(0,field);
                tuple.setField(1,new IntField((int)result));
            }
            tuples.add(tuple);
        }

        return new TupleIterator(tupleDetail, tuples);
    }

}
