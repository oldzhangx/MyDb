package mydb;

import mydb.TupleDetail.Tuple;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator{
    private static final long serialVersionUID = 5179878128589131222L;
    
    private int pageNum;
    private Iterator<Tuple> tuplesInPage;
    private TransactionId tid;

    public HeapFileIterator(TransactionId transactionId) {
        tid = transactionId;
    }

    public Iterator<Tuple> getTuplesInPage(HeapPageId pid) throws TransactionAbortedException, DbException {
        return ((HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
    }

    @Override
    public void open(HeapFile file) throws DbException, TransactionAbortedException {
        pageNum = 0;
        HeapPageId pid = new HeapPageId(file.getId(), pageNum);
        tuplesInPage = getTuplesInPage(pid);
    }

    @Override
    public boolean hasNext(HeapFile file) throws DbException, TransactionAbortedException {
        if (tuplesInPage == null) return false;
        if (tuplesInPage.hasNext()) return true;
        //TODO : page and iter relation
        if(pageNum + 1 > file.numPages())
            return false;
        pageNum = pageNum+1;
        HeapPageId pid = new HeapPageId(file.getId(), pageNum);
        tuplesInPage = getTuplesInPage(pid);
        return tuplesInPage.hasNext();
    }

    @Override
    public Tuple next(HeapFile file) throws DbException, TransactionAbortedException, NoSuchElementException {
        //TODO
        if (!hasNext(file)) throw new NoSuchElementException("not opened or no tuple remained");
        return tuplesInPage.next();
    }

    @Override
    public void rewind(HeapFile file) throws DbException, TransactionAbortedException {
        open(file);
    }

    @Override
    public void close(HeapFile file) {
        pageNum = 0;
        tuplesInPage = null;
    }
}
