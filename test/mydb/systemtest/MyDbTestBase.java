package mydb.systemtest;

import org.junit.Before;

import mydb.Database;

/**
 * Base class for all SimpleDb test classes. 
 * @author nizam
 *
 */
public class MyDbTestBase {
	/**
	 * Reset the database before each test is run.
	 */
	@Before	public void setUp() throws Exception {					
		Database.reset();
	}
	
}
