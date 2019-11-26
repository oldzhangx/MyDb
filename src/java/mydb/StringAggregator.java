package mydb;

import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;

import java.util.ArrayList;
import java.util.HashMap;

import static mydb.Type.INT_TYPE;
import static mydb.Type.STRING_TYPE;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = -4031860107066192469L;

    //the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
    int groupByFieldIndex = NO_GROUPING;

    //the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
    Type groupByFieldType = null;

    //the 0-based index of the aggregate field in the tuple
    int aggregateFieldIndex;

    //aggregation operator to use -- only supports COUNT
    Opertion opertion;

    HashMap<Field, Integer> countResult ;

    TupleDetail tupleDetail;

    /**
     * Aggregate constructor
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int groupByFieldIndex, Type groupByFieldType, int aggregateFieldIndex, Opertion what) {
        if (what!= Opertion.COUNT) throw new IllegalArgumentException("string can only deal with the COUNT case");
        this.groupByFieldIndex = groupByFieldIndex;
        this.groupByFieldType = groupByFieldType;
        this.aggregateFieldIndex = aggregateFieldIndex;
        this.opertion = what;
        countResult = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(tup == null ) throw new IllegalArgumentException("tup null error");
        int length = tup.getTupleDetail().fieldNumber();
        if(aggregateFieldIndex >= length || aggregateFieldIndex < 0)
            throw new IllegalArgumentException("aggregateFieldIndex out of field index");
        tupleDetail = tup.getTupleDetail();
        Field aggregateField = tup.getField(aggregateFieldIndex);
        if (aggregateField.getType() != STRING_TYPE) throw new IllegalArgumentException("aggregateField type isn't string_type");
        Field groupByField = groupByFieldIndex== NO_GROUPING? null: tup.getField(groupByFieldIndex);
        if( (groupByField==null && groupByFieldType== null ) ||
                (groupByField!= null && groupByFieldType!=null && groupByField.getType().equals(groupByFieldType)) ){
            Integer result = countResult.getOrDefault(groupByField,0);
            countResult.put(groupByField,result+1);
        }else throw new IllegalArgumentException("groupByFieldType not equal");
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
            if(field == null) tuple.setField(0, new IntField(countResult.get(null)));
            else {
                tuple.setField(0,field);
                tuple.setField(1,new IntField(countResult.get(field)));
            }
            tuples.add(tuple);
        }

        return new TupleIterator(tupleDetail, tuples);
    }

}
