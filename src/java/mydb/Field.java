package mydb;

import mydb.Operation.Join.Comparison;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

// define field interface
public interface Field extends Serializable {
    // inspired by simple db design
    // use dataoutputsream to write the dos into disk
    void serialize(DataOutputStream dos) throws IOException;

    // define hashCode of tuples to compare value
    int hashCode();

//    // deine value compare
//    boolean compareWith(Predicate.Op op, Field value);


    // deine value compare
    boolean compareWith(Comparison.Operation op, Field value);

    boolean equals(Object field);

    // define field type
    Type getType();

    String toString();
}
