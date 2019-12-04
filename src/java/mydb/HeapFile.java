package mydb;

import mydb.Database.BufferPool;
import mydb.Database.Database;
import mydb.Exception.DBException;
import mydb.Exception.TransactionAbortedException;
import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;


// store pages
public class HeapFile implements DbFile {

    private static final long serialVersionUID = -6321797794130816146L;

    private TupleDetail tupleDetail;
    // the file that stores the on-disk backing store for this heap
    private File file;
    private int pageCount;


    public HeapFile(File f, TupleDetail detail) {
        file =  f;
        tupleDetail = detail;
        pageCount = (int) (file.length() / BufferPool.PAGE_SIZE);
    }


    public File getFile() {
        return file;
    }

    // return a unique identify
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    //  Returns the number of pages in this HeapFile.
    public int pageCount() {
        return pageCount;
    }

    public TupleDetail getTupleDetail() {
        return tupleDetail;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IOException, DBException {
        if(pid == null) throw new DBException("readPage error:invalid page info");
        byte[] data = new byte[BufferPool.PAGE_SIZE];
        RandomAccessFile randomAccessFile = new RandomAccessFile(getFile(), "r");
        randomAccessFile.seek(pid.pageNumber() * BufferPool.PAGE_SIZE);
        randomAccessFile.read(data, 0, BufferPool.PAGE_SIZE);
        return new HeapPage((HeapPageId) pid, data);
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException, DBException {
        if(page == null) throw new DBException("invalid page info");
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek(page.getId().pageNumber() * BufferPool.PAGE_SIZE);
        byte[] data = page.getPageData();
        randomAccessFile.write(data);
    }


    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId transactionId, Tuple tuple)
            throws DBException, IOException, TransactionAbortedException {
        if(tuple == null) throw new DBException("Page insert error tuple is null");
        ArrayList<Page> pageArrayList = new ArrayList<>();
        boolean mark  = false;
        //find pages can be inserted
        for(int i = 0; i< pageCount; i++){
            // read the database by the ids
            HeapPage page = (HeapPage) Database.getBufferPool().
                    // heapPage is creates by hashcode and i in pageNo
                    getPage(transactionId, new HeapPageId(getId(),i), Permissions.READ_WRITE);
            if(page == null) throw new DBException("heapPage page is not found error");
            if(page.getNumEmptySlots() == 0) continue;
            page.insertTuple(tuple);
            page.markDirty(true,transactionId);
            pageArrayList.add(page);
            mark = true;
        }
        if(mark) return pageArrayList;
        // page is full to insert more tuples
        // create a new page and download it
        HeapPageId heapPageId = new HeapPageId(getId(),pageCount);
        // use the function
        HeapPage heapPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
        writePage(heapPage);

        HeapPage page = (HeapPage) Database.getBufferPool().
                // heapPage is creates by hashcode and pageCount in pageNo
                        getPage(transactionId, new HeapPageId(getId(),pageCount), Permissions.READ_WRITE);
        page.insertTuple(tuple);
        page.markDirty(true,transactionId);
        pageArrayList.add(page);
        pageCount++;
        return pageArrayList;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId transactionId, Tuple tuple) throws DBException,
            TransactionAbortedException, IOException {
        if(tuple == null) throw new DBException("Page delete error tuple is null");
        PageId pageId = tuple.getRecordId().getPageId();
        if(pageId.pageNumber()<pageCount) {
            HeapPage page = (HeapPage) Database.getBufferPool().
                    // heapPage is created by hashcode and i in pageNo
                            getPage(transactionId, pageId, Permissions.READ_WRITE);
            page.deleteTuple(tuple);
            page.markDirty(true,transactionId);
            return page;
        }else return null;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    public class HeapFileIterator implements DbFileIterator{
        private static final long serialVersionUID = 5179878128589131222L;

        private int pageNo;
        private TransactionId tid;
        private cachePage cachePool;

        public HeapFileIterator(TransactionId transactionId) {
            tid = transactionId;
        }

        Iterator<Tuple> getTuplesInPage(HeapPageId pid) throws TransactionAbortedException, DBException, IOException {
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (heapPage.getTupleNumber()==0) return null;
            return heapPage.iterator();
        }

        @Override
        public void open() throws TransactionAbortedException, DBException, IOException {
            pageNo = 0;
            cachePool = new cachePage(cacheRate, pageCount);
            pageNo += fillCache(pageNo);
        }

        @Override
        public boolean hasNext() throws DBException, TransactionAbortedException, IOException {
            if (cachePool == null) return false;
            if (cachePool.hasNext()) return true;

            // add new page to cache
            int addNum = fillCache(pageNo);
            if(addNum == 0) return false;
            pageNo += addNum;
            return true;
        }

        @Override
        public Tuple next() throws DBException, TransactionAbortedException, NoSuchElementException, IOException {
            if (!hasNext()) {
                throw new NoSuchElementException("not opened or no tuple remained");
            }
            return cachePool.next();
        }

        @Override
        public void rewind() throws DBException, TransactionAbortedException, IOException {
            open();
        }

        @Override
        public void close() {
            pageNo = 0;
            cachePool = null;
        }

        private final double cacheRate = 0.1;

        private int fillCache(int initPos) throws TransactionAbortedException, DBException, IOException {
            int addNum = 0;//这次调用实际添加的Page数目
            //先清空之前的缓存页
            cachePool.clear();
            int pagePos = initPos;
            for (; pagePos < pageCount && addNum < cachePool.getNum(); ) {
                HeapPageId pid = new HeapPageId(getId(), pagePos);
                Iterator<Tuple> tuples = getTuplesInPage(pid);
                if (tuples != null) {
                    cachePool.addPage(tuples);
                    addNum = ++pagePos - initPos;
                }else{
                    ++pagePos;
                }
            }
            if (addNum != 0) {
                cachePool.init();
            }
            return addNum;
        }



    }


    private class cachePage {

        private int pageNumber;

        private int index;

        private List<Iterator<Tuple>> cachePages;

        private Iterator<Tuple> current;

        /**
         * @param cacheRate 缓存的Page比例
         * @param pageNum   HeapFile所有的Page数目
         */
        public cachePage(double cacheRate, int pageNum) {
            cachePages = new ArrayList<>();
            int tmp = (int) (pageNum * cacheRate);
            pageNumber = Math.max(tmp, 1);
        }

        /**
         * 需要在缓存完page后调用
         */
        public void init() {
            if (cachePages.size() == 0) {
                throw new RuntimeException("cache has no page");
            }
            index = 0;
            current = cachePages.get(index);
        }

        public Tuple next() {
            if (!hasNext()) {
                throw new NoSuchElementException("not opened or no tuple remained");
            }
            return current.next();
        }

        public boolean hasNext() {
            if (cachePages.size() == 0 || current == null) {//还未调用addPage或init
                return false;
            }
            if (current.hasNext()) {//判断当前访问的Page是否还有tuple未访问
                return true;
            }
            if (index +1 < cachePages.size()) {//判断是否还有缓存的Page未访问
                index++;
                current = cachePages.get(index);
                return current.hasNext();
            }
            return false;
        }

        public int getNum() {
            return pageNumber;
        }

        /**
         * 清空缓存
         */
        public void clear() {
            cachePages.clear();
        }

        public void addPage(Iterator<Tuple> tuples) {
            if (cachePages.size() <= pageNumber) {
                cachePages.add(tuples);
            } else throw new RuntimeException("cache is full");
        }
    }

    public static void main(String[] args) {
        byte[] data = new byte[BufferPool.PAGE_SIZE];
        System.out.println(data.length);
        System.out.println(BufferPool.PAGE_SIZE);
    }


}

