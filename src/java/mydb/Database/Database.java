package mydb.Database;

import mydb.LogFile;
import java.io.File;
import java.io.IOException;

// db main class
// implement the database instance, catalog, bufferpool
// skeleton inspired from simple db file

//662266874
public class Database {
    // init the database instance
	private static Database _instance = new Database();

    private static final String LOG_FILE_PEX = "LOG";

	// init the catalog
    private final Catalog _catalog;

    public static Catalog getCatalog() {
        return _instance._catalog;
    }

    // init the bufferpool
    private BufferPool _bufferpool;

    public static BufferPool getBufferPool() {
        return _instance._bufferpool;
    }

    private LogFile _logfile;

    public static LogFile getLogFile() {
        return _instance._logfile;
    }

    private Database() {

    	_catalog = new Catalog();

    	_bufferpool = new BufferPool(BufferPool.DEFAULT_PAGES);
    	try{
            _logfile = new LogFile(new File(LOG_FILE_PEX));
        } catch (IOException e) {
            e.printStackTrace();
            // inspired by simple db
            // make the system safe quit from crash
            _logfile = null;
            System.exit(1);
        }
        // startControllerThread();
    }

    // TODO zhang :delete after the test
    public static BufferPool resetBufferPool(int pages) {
        _instance._bufferpool = new BufferPool(pages);
        return _instance._bufferpool;
    }

    // TODO zhang :delete after the test
    //reset the database, used for unit tests only.
    public static void reset() {
    	_instance = new Database();
    }

}
