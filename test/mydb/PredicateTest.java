package mydb;

import mydb.systemtest.MyDbTestBase;
import org.junit.Test;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import junit.framework.JUnit4TestAdapter;

public class PredicateTest extends MyDbTestBase {

  /**
   * Unit test for Predicate.filter()
   */
  @Test public void filter() {
    int[] vals = new int[] { -1, 0, 1 };

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Operation.EQUALS, TestUtil.getField(i));
      assertFalse(p.filter(Utility.getHeapTuple(i - 1)));
      assertTrue(p.filter(Utility.getHeapTuple(i)));
      assertFalse(p.filter(Utility.getHeapTuple(i + 1)));
    }

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Operation.GREATER_THAN,
          TestUtil.getField(i));
      assertFalse(p.filter(Utility.getHeapTuple(i - 1)));
      assertFalse(p.filter(Utility.getHeapTuple(i)));
      assertTrue(p.filter(Utility.getHeapTuple(i + 1)));
    }

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Operation.GREATER_THAN_OR_EQ,
          TestUtil.getField(i));
      assertFalse(p.filter(Utility.getHeapTuple(i - 1)));
      assertTrue(p.filter(Utility.getHeapTuple(i)));
      assertTrue(p.filter(Utility.getHeapTuple(i + 1)));
    }

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Operation.LESS_THAN,
          TestUtil.getField(i));
      assertTrue(p.filter(Utility.getHeapTuple(i - 1)));
      assertFalse(p.filter(Utility.getHeapTuple(i)));
      assertFalse(p.filter(Utility.getHeapTuple(i + 1)));
    }

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Operation.LESS_THAN_OR_EQ,
          TestUtil.getField(i));
      assertTrue(p.filter(Utility.getHeapTuple(i - 1)));
      assertTrue(p.filter(Utility.getHeapTuple(i)));
      assertFalse(p.filter(Utility.getHeapTuple(i + 1)));
    }
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(PredicateTest.class);
  }
}

