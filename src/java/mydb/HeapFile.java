package mydb;

import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see mydb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private static final long serialVersionUID = -6321797794130816146L;

    private TupleDetail tupleDetail;
    private File file;
    private int pageNumber;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDetail detail) {
        file =  f;
        tupleDetail = detail;
        pageNumber = (int) (file.length() / BufferPool.PAGE_SIZE) +1 ;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
        //throw new UnsupportedOperationException("get hash table id fail");
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return pageNumber;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDetail getTupleDesc() {
        return tupleDetail;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IOException {
        byte[] data = new byte[BufferPool.PAGE_SIZE];
        RandomAccessFile randomAccessFile = new RandomAccessFile(getFile(), "r");
        randomAccessFile.seek(pid.pageNumber() * BufferPool.PAGE_SIZE);
        randomAccessFile.read(data, 0, data.length);
        return new HeapPage((HeapPageId) pid, data);
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }



    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    public class HeapFileIterator implements DbFileIterator{
        private static final long serialVersionUID = 5179878128589131222L;

        private int pageNum;
        private Iterator<Tuple> tuplesInPage;
        private TransactionId tid;

        public HeapFileIterator(TransactionId transactionId) {
            tid = transactionId;
        }

        public Iterator<Tuple> getTuplesInPage(HeapPageId pid) throws TransactionAbortedException, DbException, IOException {
            return ((HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException, IOException {
            pageNum = 0;
            HeapPageId pid = new HeapPageId(getId(), pageNum);
            tuplesInPage = getTuplesInPage(pid);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException, IOException {
            if (tuplesInPage == null) return false;
            if (tuplesInPage.hasNext()) return true;
            //TODO : page and iter relation
            if(pageNum + 1 > numPages())
                return false;
            pageNum = pageNum+1;
            HeapPageId pid = new HeapPageId(getId(), pageNum);
            tuplesInPage = getTuplesInPage(pid);
            return tuplesInPage.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException, IOException {
            //TODO
            if (!hasNext()) throw new NoSuchElementException("not opened or no tuple remained");
            return tuplesInPage.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException, IOException {
            open();
        }

        @Override
        public void close() {
            pageNum = 0;
            tuplesInPage = null;
        }
    }


}

