package mydb;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;

public enum Type implements Serializable {

    INT_TYPE() {
        @Override
        public int getLen() {
            return 4;
        }

        @Override
        public Field parse (DataInputStream dataInputStream) throws ParseException {
            try{
                return new IntField(dataInputStream.readInt());
            } catch (IOException e) {
                e.printStackTrace();
                throw new ParseException("int parse error", 0);
            }
        }
    },

    LONG_TYPE() {
        @Override
        public int getLen() {
            return 16;
        }

        @Override
        public Field parse (DataInputStream dataInputStream) throws ParseException {
            try{
                return new LongField(dataInputStream.readLong());
            } catch (IOException e) {
                e.printStackTrace();
                throw new ParseException("long parse error", 0);
            }
        }
    },

    STRING_TYPE() {
        @Override
        public int getLen() {
            return STRING_LEN + 4;
        }

        @Override
        public Field parse(DataInputStream dataInputStream) throws ParseException {

            try{
                // read first four is string's length
                int length = dataInputStream.readInt();
                byte[] bytes = new byte[length];

                dataInputStream.read(bytes);
                dataInputStream.skipBytes(STRING_LEN -length);

                return new StringField(new String(bytes), STRING_LEN);
            } catch (IOException e) {
                e.printStackTrace();
                throw new ParseException("String parse error", 0);
            }

        }
    };
    
    public static final int STRING_LEN = 128;

    public abstract int getLen();

    // read
    public abstract Field parse(DataInputStream dis) throws ParseException;

}
