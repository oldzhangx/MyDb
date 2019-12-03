package mydb.Database;

import mydb.*;
import mydb.Exception.DBException;
import mydb.Exception.TransactionAbortedException;
import mydb.TupleDetail.Tuple;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;


// manages the reading and writing of pages into memory from disk
public class BufferPool {

    //Bytes per page
    public static final int PAGE_SIZE = 4096;

    //Default number of pages passed to the constructor
    public static final int DEFAULT_PAGES = 100;

//    private HashMap<PageId, Page> pages;

    //当前的缓存页
    private LRUCache lruCache;

    public int PAGES_NUM;

    // create bufferPool
    public BufferPool(int numPages) {
//        pages = new HashMap<>(numPages);
        PAGES_NUM = numPages;
        lruCache = new LRUCache(PAGES_NUM);
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DBException, IOException {
        if (lruCache != null && lruCache.get(pid)!=null)
            return lruCache.get(pid);
        DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
        HeapPage newPage = (HeapPage) dbFile.readPage(pid);
        lruCache.put(pid, newPage);

//        if (pages.size() > NUM_PAGES) {
//            // TODO: implement this
//        }
        return newPage;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     */
    public void insertTuple(TransactionId transactionId, int tableId, Tuple tuple)
        throws DBException, IOException, TransactionAbortedException {

        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(tableId);
        table.insertTuple(transactionId, tuple);
        //ArrayList<Page> pageArrayList = table.insertTuple(transactionId, tuple);
//        for (Page page : pageArrayList) {
//            page.markDirty(true, tid);
//        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     */
    public  void deleteTuple(TransactionId transactionId, Tuple tuple)
            throws DBException, TransactionAbortedException, IOException {
        int tableId=tuple.getRecordId().getPageId().getTableId();
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(tableId);
        table.deleteTuple(transactionId, tuple);
        //Page affectedPage = table.deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {


    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DBException {
        // some code goes here
        // not necessary for proj1
    }

}
