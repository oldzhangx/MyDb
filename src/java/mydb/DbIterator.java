package mydb;
import mydb.Exception.DBException;
import mydb.Exception.TransactionAbortedException;
import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;

import java.io.IOException;
import java.io.Serializable;
import java.util.NoSuchElementException;


public interface DbIterator extends Serializable{

  // Opens the iterator. This must be called before any of the other methods.
  public void open() throws DBException, TransactionAbortedException, IOException;

  // true if the iterator has more tuples.
  public boolean hasNext() throws DBException, TransactionAbortedException, IOException;

  // Returns the next tuple from the operator
  public Tuple next() throws DBException, TransactionAbortedException, NoSuchElementException, IOException;


  //Resets the iterator to the start.
  public void rewind() throws DBException, TransactionAbortedException, IOException;


  //the TupleDetail associated with this DbIterator.
  public TupleDetail getTupleDetail();

  public void close();

}
