package mydb;

import java.util.NoSuchElementException;

import mydb.TupleDetail.TupleDetail;
import org.junit.Test;

import mydb.systemtest.MyDbTestBase;

import static org.junit.Assert.*;
import junit.framework.Assert;
import junit.framework.JUnit4TestAdapter;

public class TupleDetailTest extends MyDbTestBase {

    /**
     * Unit test for TupleDesc.combine()
     */
    @Test public void combine() {
        TupleDetail td1, td2, td3;

        td1 = Utility.getTupleDesc(1, "td1");
        td2 = Utility.getTupleDesc(2, "td2");

        // test td1.combine(td2)
        td3 = TupleDetail.merge(td1, td2);
        assertEquals(3 , td3.fieldNumber());
        assertEquals(3 * Type.INT_TYPE.getLen(), td3.getSize());
        for (int i = 0; i < 3; ++i)
            assertEquals(Type.INT_TYPE, td3.getFieldType(i));
        assertTrue(combinedStringArrays(td1, td2, td3));

        // test td2.combine(td1)
        td3 = TupleDetail.merge(td2, td1);
        assertEquals(3 , td3.fieldNumber());
        assertEquals(3 * Type.INT_TYPE.getLen(), td3.getSize());
        for (int i = 0; i < 3; ++i)
            assertEquals(Type.INT_TYPE, td3.getFieldType(i));
        assertTrue(combinedStringArrays(td2, td1, td3));

        // test td2.combine(td2)
        td3 = TupleDetail.merge(td2, td2);
        assertEquals(4 , td3.fieldNumber());
        assertEquals(4 * Type.INT_TYPE.getLen(), td3.getSize());
        for (int i = 0; i < 4; ++i)
            assertEquals(Type.INT_TYPE, td3.getFieldType(i));
        assertTrue(combinedStringArrays(td2, td2, td3));
    }

    /**
     * Ensures that combined's field names = td1's field names + td2's field names
     */
    private boolean combinedStringArrays(TupleDetail td1, TupleDetail td2, TupleDetail combined) {
        for (int i = 0; i < td1.fieldNumber(); i++) {
            if (!(((td1.getFieldName(i) == null) && (combined.getFieldName(i) == null)) ||
                    td1.getFieldName(i).equals(combined.getFieldName(i)))) {
                return false;
            }
        }

        for (int i = td1.fieldNumber(); i < td1.fieldNumber() + td2.fieldNumber(); i++) {
            if (!(((td2.getFieldName(i-td1.fieldNumber()) == null) && (combined.getFieldName(i) == null)) ||
                    td2.getFieldName(i-td1.fieldNumber()).equals(combined.getFieldName(i)))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Unit test for TupleDesc.getType()
     */
    @Test public void getType() {
        int[] lengths = new int[] { 1, 2, 1000 };

        for (int len: lengths) {
            TupleDetail td = Utility.getTupleDesc(len);
            for (int i = 0; i < len; ++i)
                assertEquals(Type.INT_TYPE, td.getFieldType(i));
        }
    }
    
    /**
     * Unit test for TupleDesc.nameToId()
     */
    @Test public void nameToId() {
        int[] lengths = new int[] { 1, 2, 1000 };
        String prefix = "test";
        
        for (int len: lengths) {
            // Make sure you retrieve well-named fields
            TupleDetail td = Utility.getTupleDesc(len, prefix);
            for (int i = 0; i < len; ++i) {
                assertEquals(i, td.fieldNameToIndex(prefix + i));
            }
            
            // Make sure you throw exception for non-existent fields
            try {
                td.fieldNameToIndex("foo");
                Assert.fail("foo is not a valid field name");
            } catch (NoSuchElementException e) {
                // expected to get here
            }
            
            // Make sure you throw exception for null searches
            try {
                td.fieldNameToIndex(null);
                Assert.fail("null is not a valid field name");
            } catch (NoSuchElementException e) {
                // expected to get here
            }

            // Make sure you throw exception when all field names are null
            td = Utility.getTupleDesc(len);
            try {
                td.fieldNameToIndex(prefix);
                Assert.fail("no fields are named, so you can't find it");
            } catch (NoSuchElementException e) {
                // expected to get here
            }
        }
    }    

    /**
     * Unit test for TupleDesc.getSize()
     */
    @Test public void getSize() {
        int[] lengths = new int[] { 1, 2, 1000 };

        for (int len: lengths) {
            TupleDetail td = Utility.getTupleDesc(len);
            assertEquals(len * Type.INT_TYPE.getLen(), td.getSize());
        }
    }

    /**
     * Unit test for TupleDesc.numFields()
     */
    @Test public void numFields() {
        int[] lengths = new int[] { 1, 2, 1000 };

        for (int len : lengths) {
            TupleDetail td = Utility.getTupleDesc(len);
            assertEquals(len, td.fieldNumber());
        }
    }

    @Test public void testEquals() {
        TupleDetail singleInt = new TupleDetail(new Type[]{Type.INT_TYPE});
        TupleDetail singleInt2 = new TupleDetail(new Type[]{Type.INT_TYPE});
        TupleDetail intString = new TupleDetail(new Type[]{Type.INT_TYPE, Type.STRING_TYPE});

        // .equals() with null should return false
        assertFalse(singleInt.equals(null));

        // .equals() with the wrong type should return false
        assertFalse(singleInt.equals(new Object()));

        assertTrue(singleInt.equals(singleInt));
        assertTrue(singleInt.equals(singleInt2));
        assertTrue(singleInt2.equals(singleInt));
        assertEquals(intString, intString);

        assertFalse(singleInt.equals(intString));
        assertFalse(singleInt2.equals(intString));
        assertFalse(intString.equals(singleInt));
        assertFalse(intString.equals(singleInt2));
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(TupleDetailTest.class);
    }
}

