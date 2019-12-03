package mydb.systemtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

import mydb.*;
import mydb.Database.Database;
import mydb.Exception.DBException;
import mydb.Exception.TransactionAbortedException;
import mydb.Operation.Join.Comparison;
import org.junit.Test;



public abstract class FilterBase extends MyDbTestBase {
    private static final int COLUMNS = 3;
    private static final int ROWS = 1097;

    /** Should apply the predicate to table. This will be executed in transaction tid. */
    protected abstract int applyPredicate(HeapFile table, TransactionId tid, Comparison comparison)
            throws DBException, TransactionAbortedException, IOException;

    /** Optional hook for validating database state after applyPredicate. */
    protected void validateAfter(HeapFile table)
            throws DBException, TransactionAbortedException, IOException {}

    protected ArrayList<ArrayList<Integer>> createdTuples;

    private int runTransactionForPredicate(HeapFile table, Comparison comparison)
            throws IOException, DBException, TransactionAbortedException {
        TransactionId tid = new TransactionId();
        int result = applyPredicate(table, tid, comparison);
        Database.getBufferPool().transactionComplete(tid);
        return result;
    }

    private void validatePredicate(int column, int columnValue, int trueValue, int falseValue,
            Comparison.Operation operation) throws IOException, DBException, TransactionAbortedException {
        // Test the true value
        HeapFile f = createTable(column, columnValue);
        Comparison comparison = new Comparison(column, operation, new IntField(trueValue));
        assertEquals(ROWS, runTransactionForPredicate(f, comparison));
        f = Utility.openHeapFile(COLUMNS, f.getFile());
        validateAfter(f);

        // Test the false value
        f = createTable(column, columnValue);
        comparison = new Comparison(column, operation, new IntField(falseValue));
        assertEquals(0, runTransactionForPredicate(f, comparison));
        f = Utility.openHeapFile(COLUMNS, f.getFile());
        validateAfter(f);
    }

    private HeapFile createTable(int column, int columnValue)
            throws IOException, DBException, TransactionAbortedException {
        Map<Integer, Integer> columnSpecification = new HashMap<Integer, Integer>();
        columnSpecification.put(column, columnValue);
        createdTuples = new ArrayList<ArrayList<Integer>>();
        return SystemTestUtil.createRandomHeapFile(
                COLUMNS, ROWS, columnSpecification, createdTuples);
    }

    @Test public void testEquals() throws
            DBException, TransactionAbortedException, IOException {
        validatePredicate(0, 1, 1, 2, Comparison.Operation.EQUALS);
    }

    @Test public void testLessThan() throws
            DBException, TransactionAbortedException, IOException {
        validatePredicate(1, 1, 2, 1, Comparison.Operation.LESS_THAN);
    }

    @Test public void testLessThanOrEq() throws
            DBException, TransactionAbortedException, IOException {
        validatePredicate(2, 42, 42, 41, Comparison.Operation.LESS_THAN_OR_EQ);
    }

    @Test public void testGreaterThan() throws
            DBException, TransactionAbortedException, IOException {
        validatePredicate(2, 42, 41, 42, Comparison.Operation.GREATER_THAN);
    }

    @Test public void testGreaterThanOrEq() throws
            DBException, TransactionAbortedException, IOException {
        validatePredicate(2, 42, 42, 43, Comparison.Operation.GREATER_THAN_OR_EQ);
    }
}
