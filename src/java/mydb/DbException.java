package mydb;

import java.lang.Exception;

/** Generic database exception class */
public class DbException extends Exception {

    private static final long serialVersionUID = 5471074685023125529L;

    public DbException(String s) {
        super(s);
    }
}
