package mydb;

import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;
import java.io.IOException;
import java.util.HashMap;
import java.util.NoSuchElementException;

import static mydb.Type.INT_TYPE;

public class Aggregate extends Operator {

    private static final long serialVersionUID = 6394662876144848862L;

    //the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
    int groupByFieldIndex;

    //the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
    Type groupByFieldType = null;

    //the 0-based index of the aggregate field in the tuple
    int aggregateFieldIndex;

    //aggregation operator to use -- only supports COUNT
    Aggregator.Opertion opertion;

    HashMap<Field, Integer> countResult ;

    TupleDetail tupleDetail;

    DbIterator child;

    DbIterator aggregateIter;

    Aggregator aggregator;


    public Aggregate(DbIterator child, int aggregateFieldIndex, int groupByFieldIndex, Aggregator.Opertion aop) {
        this.child = child;
        this.aggregateFieldIndex = aggregateFieldIndex;
        this.groupByFieldIndex = groupByFieldIndex;
        opertion = aop;
        tupleDetail = child.getTupleDetail();
        Type aggreType = child.getTupleDetail().getFieldType(aggregateFieldIndex);
        //根据进行聚合的列的类型来判断aggreator的类型
        groupByFieldType = groupByFieldIndex == Aggregator.NO_GROUPING ? null : child.getTupleDetail().getFieldType(groupByFieldIndex);
        if (aggreType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(groupByFieldIndex, groupByFieldType, aggregateFieldIndex, aop);
        } else if (aggreType == Type.STRING_TYPE) {
            aggregator = new StringAggregator(groupByFieldIndex, groupByFieldType, aggregateFieldIndex, aop);
        }

    }
//
//    public int groupField() {
//	    return groupByFieldIndex;
//    }
//
//    /**
//     * @return If this aggregate is accompanied by a group by, return the name
//     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
//     *         null;
//     * */
//    public String groupFieldName() {
//	// some code goes here
//	return null;
//    }
//
//    /**
//     * @return the aggregate field
//     * */
//    public int aggregateField() {
//	// some code goes here
//	return -1;
//    }
//
//    /**
//     * @return return the name of the aggregate field in the <b>OUTPUT</b>
//     *         tuples
//     * */
//    public String aggregateFieldName() {
//	// some code goes here
//	return null;
//    }
//
//    /**
//     * @return return the aggregate operator
//     * */
//    public Aggregator.Opertion aggregateOp() {
//	// some code goes here
//	return null;
//    }

    public static String nameOfAggregatorOp(Aggregator.Opertion aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException, IOException {
        child.open();
        super.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        aggregateIter = aggregator.iterator();
        aggregateIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException, IOException {
        if (aggregateIter.hasNext()) {
            return aggregateIter.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException, IOException {
        aggregateIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDetail getTupleDetail() {
        Type[] fieldType = groupByFieldType==null? new Type[]{INT_TYPE}:
                new Type[]{groupByFieldType, INT_TYPE};
        String[] fieldName = groupByFieldType==null? new String[]{tupleDetail.getFieldName(aggregateFieldIndex)}:
                new String[]{tupleDetail.getFieldName(groupByFieldIndex), tupleDetail.getFieldName(aggregateFieldIndex)};
        return new TupleDetail(fieldType, fieldName);
    }

    public void close() {
        super.close();
        aggregateIter.close();
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    }
    
}
