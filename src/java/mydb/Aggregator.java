package mydb;

import mydb.TupleDetail.Tuple;

import java.io.Serializable;


public interface Aggregator extends Serializable {

    // other index means 0-based index
    int NO_GROUPING = -1;

    enum Opertion implements Serializable {
        MIN, MAX, SUM, AVG, COUNT;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Opertion getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
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

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup);

    public DbIterator iterator();
    
}
