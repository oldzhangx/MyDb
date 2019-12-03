package mydb.Operation;

import mydb.DbIterator;
import mydb.TupleDetail.Tuple;
import java.io.Serializable;


public interface Aggregator extends Serializable {

    // other index means 0-based index
    int NO_GROUPING = -1;

    enum Opertion implements Serializable {
        MIN, MAX, SUM, AVG, COUNT;

        // a string containing a valid integer Op index
        public static Opertion getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        // i a valid integer Op index
        public static Opertion getOp(int i) {
            return values()[i];
        }
        
        public String toString()
        {
        	if (this==MIN)
        		return "min";
        	if (this==MAX)
        		return "max";
        	if (this==SUM)
        		return "sum";
        	if (this==AVG)
        		return "avg";
        	if (this==COUNT)
        		return "count";
        	throw new IllegalStateException("impossible to reach here");
        }
    }

    // Merge a new tuple into the aggregate for a distinct group value
    public void mergeTupleIntoGroup(Tuple tup);

    public DbIterator iterator();
    
}
