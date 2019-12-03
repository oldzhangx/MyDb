package mydb;

import java.io.DataOutputStream;
import java.io.IOException;


public class StringField implements Field {

	private static final long serialVersionUID = -8875396221418374446L;

	private String value;

	//define maxSize to ensure the length of string
	private int maxSize;

	public String getValue() {
		return value;
	}

	public StringField(String s, int size) {
		maxSize = size;
		value = s.length() > maxSize? s.substring(0, maxSize):s;
	}


	// file storage = 4 bytes size storage + string value
	public void serialize(DataOutputStream dataOutputStream) throws IOException {
		String s = value;
		int ex = maxSize - s.length();
		s = ex>=0? s: s.substring(0, maxSize);
		try{
			// write s.length in the file
			dataOutputStream.writeInt(s.length());
			// write string in the file
			dataOutputStream.writeBytes(s);
			// fill with 0
			while(ex-->0)
				dataOutputStream.writeByte((byte)0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean compareWith(Comparison.Operation operation, Field field) {
		StringField stringField = (StringField)field;
		int cmpVal = value.compareTo(stringField.value);

		switch (operation) {
			case EQUALS:
				return cmpVal == 0;

			case NOT_EQUALS:
				return cmpVal != 0;

			case GREATER_THAN:
				return cmpVal > 0;

			case GREATER_THAN_OR_EQ:
				return cmpVal >= 0;

			case LESS_THAN:
				return cmpVal < 0;

			case LESS_THAN_OR_EQ:
				return cmpVal <= 0;

			case LIKE:
				return value.contains(stringField.value);
		}
		throw new IllegalArgumentException("string compare error");
	}

	public boolean equals(Object field) {
		return value.equals(((StringField)field).value);
	}

	public Type getType() {
		return Type.STRING_TYPE;
	}

	public String toString() {
		return value;
	}

	public int hashCode() {
		return value.hashCode();
	}
}
