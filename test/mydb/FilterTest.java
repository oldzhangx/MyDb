package mydb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;
import mydb.systemtest.MyDbTestBase;
import org.junit.Before;
import org.junit.Test;


public class FilterTest extends MyDbTestBase {

  int testWidth = 3;
  DbIterator scan;

  /**
   * Initialize each unit test
   */
  @Before public void setUp() {
    this.scan = new TestUtil.MockScan(-5, 5, testWidth);
  }

  /**
   * Unit test for Filter.getTupleDesc()
   */
  @Test public void getTupleDesc() {
    Comparison pred = new Comparison(0, Comparison.Operation.EQUALS, TestUtil.getField(0));
    Filter op = new Filter(pred, scan);
    TupleDetail expected = Utility.getTupleDesc(testWidth);
    TupleDetail actual = op.getTupleDetail();
    assertEquals(expected, actual);
  }

  /**
   * Unit test for Filter.rewind()
   */
  @Test public void rewind() throws Exception {
    Comparison pred = new Comparison(0, Comparison.Operation.EQUALS, TestUtil.getField(0));
    Filter op = new Filter(pred, scan);
    op.open();
    assertTrue(op.hasNext());
    assertNotNull(op.next());
    assertTrue(TestUtil.checkExhausted(op));

    op.rewind();
    Tuple expected = Utility.getHeapTuple(0, testWidth);
    Tuple actual = op.next();
    assertTrue(TestUtil.compareTuples(expected, actual));
    op.close();
  }

  /**
   * Unit test for Filter.getNext() using a &lt; predicate that filters
   *   some tuples
   */
  @Test public void filterSomeLessThan() throws Exception {
    Comparison pred;
    pred = new Comparison(0, Comparison.Operation.LESS_THAN, TestUtil.getField(2));
    Filter op = new Filter(pred, scan);
    TestUtil.MockScan expectedOut = new TestUtil.MockScan(-5, 2, testWidth);
    op.open();
    TestUtil.compareDbIterators(op, expectedOut);
    op.close();
  }

  /**
   * Unit test for Filter.getNext() using a &lt; predicate that filters
   * everything
   */
  @Test public void filterAllLessThan() throws Exception {
    Comparison pred;
    pred = new Comparison(0, Comparison.Operation.LESS_THAN, TestUtil.getField(-5));
    Filter op = new Filter(pred, scan);
    op.open();
    assertTrue(TestUtil.checkExhausted(op));
    op.close();
  }

  /**
   * Unit test for Filter.getNext() using an = predicate
   */
  @Test public void filterEqual() throws Exception {
    Comparison pred;
    this.scan = new TestUtil.MockScan(-5, 5, testWidth);
    pred = new Comparison(0, Comparison.Operation.EQUALS, TestUtil.getField(-5));
    Filter op = new Filter(pred, scan);
    op.open();
    assertTrue(TestUtil.compareTuples(Utility.getHeapTuple(-5, testWidth),
        op.next()));
    op.close();

    this.scan = new TestUtil.MockScan(-5, 5, testWidth);
    pred = new Comparison(0, Comparison.Operation.EQUALS, TestUtil.getField(0));
    op = new Filter(pred, scan);
    op.open();
    assertTrue(TestUtil.compareTuples(Utility.getHeapTuple(0, testWidth),
        op.next()));
    op.close();

    this.scan = new TestUtil.MockScan(-5, 5, testWidth);
    pred = new Comparison(0, Comparison.Operation.EQUALS, TestUtil.getField(4));
    op = new Filter(pred, scan);
    op.open();
    assertTrue(TestUtil.compareTuples(Utility.getHeapTuple(4, testWidth),
        op.next()));
    op.close();
  }

  /**
   * Unit test for Filter.getNext() using an = predicate passing no tuples
   */
  @Test public void filterEqualNoTuples() throws Exception {
    Comparison pred;
    pred = new Comparison(0, Comparison.Operation.EQUALS, TestUtil.getField(5));
    Filter op = new Filter(pred, scan);
    op.open();
    TestUtil.checkExhausted(op);
    op.close();
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(FilterTest.class);
  }
}

