package mydb.Exception;
import java.lang.Exception;

public class ParserException extends Exception {
    private static final long serialVersionUID = -2821885792796776014L;

    public ParserException(String string) {
        super("ParserException: "+string);
    }

    public ParserException(Exception e) {
        super(e);
    }

}
