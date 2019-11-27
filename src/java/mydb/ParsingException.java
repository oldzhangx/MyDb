package mydb;
import java.lang.Exception;

public class ParsingException extends Exception {
    private static final long serialVersionUID = -2821885792796776014L;

    public ParsingException(String string) {
        super(string);
    }

    public ParsingException(Exception e) {
        super(e);
    }

}
