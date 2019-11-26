package mydb;

public class HeapPageId implements PageId {

    private int tableId;
    private int pageNumber;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tId, int pgNo) {
        tableId = tId;
        pageNumber = pgNo;
    }

    public int getTableId() {
        return tableId;
    }

    //the page number in the table getTableId() associated with this PageId
    public int pageNumber() {
        return pageNumber;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        return 31 * tableId + pageNumber;
    }

    public boolean equals(Object o) {
        if(this == o) return true;
        if(o == null) return false;
        if(! (o instanceof PageId)) return false;
        PageId pageId = (PageId) o;
        return pageNumber == pageId.pageNumber() && tableId == pageId.getTableId();
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        return new int[]{tableId, pageNumber};
    }

}
