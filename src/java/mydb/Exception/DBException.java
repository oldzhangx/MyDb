package mydb.Exception;

import java.lang.Exception;

public class DBException extends Exception {

    private static final long serialVersionUID = 5471074685023125529L;

    public DBException(String string) {
        super("DBException: " + string);
    }
}
