package mydb;

import Zql.*;
import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.SimpleCompletor;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import mydb.Database.Database;
import mydb.TupleDetail.Tuple;
import mydb.TupleDetail.TupleDetail;

public class Parser {
    private static boolean explain = false;

    public static Comparison.Operation getOp(String s) throws mydb.ParsingException, ParsingException {
        if (s.equals("="))
            return Comparison.Operation.EQUALS;
        if (s.equals(">"))
            return Comparison.Operation.GREATER_THAN;
        if (s.equals(">="))
            return Comparison.Operation.GREATER_THAN_OR_EQ;
        if (s.equals("<"))
            return Comparison.Operation.LESS_THAN;
        if (s.equals("<="))
            return Comparison.Operation.LESS_THAN_OR_EQ;
        if (s.equals("LIKE"))
            return Comparison.Operation.LIKE;
        if (s.equals("~"))
            return Comparison.Operation.LIKE;
        if (s.equals("<>"))
            return Comparison.Operation.NOT_EQUALS;
        if (s.equals("!="))
            return Comparison.Operation.NOT_EQUALS;

        throw new mydb.ParsingException("Unknown predicate " + s);
    }

    void processExpression(TransactionId tid, ZExpression wx, LogicalPlan lp)
            throws mydb.ParsingException {
        if (wx.getOperator().equals("AND")) {
            for (int i = 0; i < wx.nbOperands(); i++) {
                if (!(wx.getOperand(i) instanceof ZExpression)) {
                    throw new mydb.ParsingException(
                            "Nested queries are currently unsupported.");
                }
                ZExpression newWx = (ZExpression) wx.getOperand(i);
                processExpression(tid, newWx, lp);

            }
        } else if (wx.getOperator().equals("OR")) {
            throw new mydb.ParsingException(
                    "OR expressions currently unsupported.");
        } else {
            // this is a binary expression comparing two constants
            @SuppressWarnings("unchecked")
            Vector<ZExp> ops = wx.getOperands();
            if (ops.size() != 2) {
                throw new mydb.ParsingException(
                        "Only simple binary expresssions of the form A op B are currently supported.");
            }

            boolean isJoin = false;
            Comparison.Operation op = getOp(wx.getOperator());

            boolean op1const = ops.elementAt(0) instanceof ZConstant; // otherwise
                                                                      // is a
                                                                      // Query
            boolean op2const = ops.elementAt(1) instanceof ZConstant; // otherwise
                                                                      // is a
                                                                      // Query
            if (op1const && op2const) {
                isJoin = ((ZConstant) ops.elementAt(0)).getType() == ZConstant.COLUMNNAME
                        && ((ZConstant) ops.elementAt(1)).getType() == ZConstant.COLUMNNAME;
            } else if (ops.elementAt(0) instanceof ZQuery
                    || ops.elementAt(1) instanceof ZQuery) {
                isJoin = true;
            } else if (ops.elementAt(0) instanceof ZExpression
                    || ops.elementAt(1) instanceof ZExpression) {
                throw new mydb.ParsingException(
                        "Only simple binary expresssions of the form A op B are currently supported, where A or B are fields, constants, or subqueries.");
            } else
                isJoin = false;

            if (isJoin) { // join node

                String tab1field = "", tab2field = "";

                if (!op1const) { // left op is a nested query
                    // generate a virtual table for the left op
                    // this isn't a valid ZQL query
                } else {
                    tab1field = ((ZConstant) ops.elementAt(0)).getValue();

                }

                if (!op2const) { // right op is a nested query
                    try {
                        LogicalPlan sublp = parseQueryLogicalPlan(tid,
                                (ZQuery) ops.elementAt(1));
                        DbIterator pp = sublp.physicalPlan(tid,
                                TableStats.getStatsMap(), explain);
                        lp.addJoin(tab1field, pp, op);
                    } catch (IOException | ParseException e) {
                        throw new mydb.ParsingException("Invalid subquery "
                                + ops.elementAt(1));
                    }
                } else {
                    tab2field = ((ZConstant) ops.elementAt(1)).getValue();
                    lp.addJoin(tab1field, tab2field, op);
                }

            } else { // select node
                String column;
                String compValue;
                ZConstant op1 = (ZConstant) ops.elementAt(0);
                ZConstant op2 = (ZConstant) ops.elementAt(1);
                if (op1.getType() == ZConstant.COLUMNNAME) {
                    column = op1.getValue();
                    compValue = new String(op2.getValue());
                } else {
                    column = op2.getValue();
                    compValue = new String(op1.getValue());
                }

                lp.addFilter(column, op, compValue);

            }
        }

    }

    public LogicalPlan parseQueryLogicalPlan(TransactionId tid, ZQuery q)
            throws IOException, Zql.ParseException, mydb.ParsingException {
        @SuppressWarnings("unchecked")
        Vector<ZFromItem> from = q.getFrom();
        LogicalPlan lp = new LogicalPlan();
        lp.setQuery(q.toString());
        // walk through tables in the FROM clause
        for (int i = 0; i < from.size(); i++) {
            ZFromItem fromIt = from.elementAt(i);
            try {

                int id = Database.getCatalog().getTableId(fromIt.getTable()); // will
                // fall
                // through
                // if
                // table
                // doesn't
                // exist
                String name;
                //R: name is either Students or s
                if (fromIt.getAlias() != null)
                    name = fromIt.getAlias();
                else
                    name = fromIt.getTable();
                //R: add a scan to the logicalPlan for the table
                lp.addScan(id, name);

                // XXX handle subquery?
            } catch (NoSuchElementException e) {
                e.printStackTrace();
                throw new mydb.ParsingException("Table "
                        + fromIt.getTable() + " is not in catalog");
            }
        }

        // now parse the where clause, creating Filter and Join nodes as needed
        ZExp w = q.getWhere();
        if (w != null) {
            //R: no nested Query
            if (!(w instanceof ZExpression)) {
                throw new mydb.ParsingException(
                        "Nested queries are currently unsupported.");
            }
            ZExpression wx = (ZExpression) w;
            processExpression(tid, wx, lp);

        }

        // now look for group by fields

        //R: ZQuery.getGroupBy() gets the GROUP BY...HAVING part of the expression, which is different from ZGroupBy.getGroupBy(), which gets only the GROUP BY part of the group by expression
        ZGroupBy gby = q.getGroupBy();
        String groupByField = null;
        if (gby != null) {
            @SuppressWarnings("unchecked")
            Vector<ZExp> gbs = gby.getGroupBy();
            //R: only can have 1 GROUP BY
            if (gbs.size() > 1) {
                throw new mydb.ParsingException(
                        "At most one grouping field expression supported.");
            }
            if (gbs.size() == 1) {
                ZExp gbe = gbs.elementAt(0);
                if (!(gbe instanceof ZConstant)) {
                    throw new mydb.ParsingException(
                            "Complex grouping expressions (" + gbe
                                    + ") not supported.");
                }
                //R: get GROUP BY field
                groupByField = ((ZConstant) gbe).getValue();
                System.out.println("GROUP BY FIELD : " + groupByField);
            }

        }

        // walk the select list, pick out aggregates, and check for query
        // validity
        @SuppressWarnings("unchecked")
        Vector<ZSelectItem> selectList = q.getSelect();
        String aggField = null;
        String aggFun = null;

        for (int i = 0; i < selectList.size(); i++) {
            ZSelectItem si = selectList.elementAt(i);
            if (si.getAggregate() == null
                    && (si.isExpression() && !(si.getExpression() instanceof ZConstant))) {
                throw new mydb.ParsingException(
                        "Expressions in SELECT list are not supported.");
            }
            if (si.getAggregate() != null) {
                if (aggField != null) {
                    throw new mydb.ParsingException(
                            "Aggregates over multiple fields not supported.");
                }
                //R: gets the aggregate's function and field, e.g. COUNT(sid)
                aggField = ((ZConstant) ((ZExpression) si.getExpression())
                        .getOperand(0)).getValue();
                aggFun = si.getAggregate();
                System.out.println("Aggregate field is " + aggField
                        + ", agg fun is : " + aggFun);
                //R: add a projection of a field and a aggregate to the logical plan
                lp.addProjectField(aggField, aggFun);
            } else {
                if (groupByField != null
                        && !(groupByField.equals(si.getTable() + "."
                        + si.getColumn()) || groupByField.equals(si
                        .getColumn()))) {
                    throw new mydb.ParsingException("Non-aggregate field "
                            + si.getColumn()
                            + " does not appear in GROUP BY list.");
                }
                //R: add a simple projection of a filed to the lp
                lp.addProjectField(si.getTable() + "." + si.getColumn(), null);
            }
        }

        if (groupByField != null && aggFun == null) {
            throw new mydb.ParsingException("GROUP BY without aggregation.");
        }

        if (aggFun != null) {
            //R:Add an aggregate over the field with the specified grouping to the query.
            lp.addAggregate(aggFun, aggField, groupByField);
        }
        // sort the data

        if (q.getOrderBy() != null) {
            @SuppressWarnings("unchecked")
            Vector<ZOrderBy> obys = q.getOrderBy();
            if (obys.size() > 1) {
                throw new mydb.ParsingException(
                        "Multi-attribute ORDER BY is not supported.");
            }
            ZOrderBy oby = obys.elementAt(0);
            if (!(oby.getExpression() instanceof ZConstant)) {
                throw new mydb.ParsingException(
                        "Complex ORDER BY's are not supported");
            }
            ZConstant f = (ZConstant) oby.getExpression();
            //R: add orderBy to the logical plan

            lp.addOrderBy(f.getValue(), oby.getAscOrder());

        }
        return lp;
    }

    private Transaction curtrans = null;
    private boolean inUserTrans = false;

    public Query handleQueryStatement(ZQuery zQuery, TransactionId tId)
            throws TransactionAbortedException, DbException, IOException,
            mydb.ParsingException, Zql.ParseException {
        // and run it
        Query query = new Query(tId);

        LogicalPlan lp = parseQueryLogicalPlan(tId, zQuery);
        DbIterator physicalPlan = lp.physicalPlan(tId,
                TableStats.getStatsMap(), explain);
        query.setPhysicalPlan(physicalPlan);
        query.setLogicalPlan(lp);

        if (physicalPlan != null) {
            Class<?> c;
            try {
                c = Class.forName("simpledb.OperatorCardinality");

                Class<?> p = Operator.class;
                Class<?> h = Map.class;

                java.lang.reflect.Method m = c.getMethod(
                        "updateOperatorCardinality", p, h, h);

                System.out.println("The query plan is:");
                m.invoke(null, (Operator) physicalPlan,
                        lp.getTableAliasToIdMapping(), TableStats.getStatsMap());
                c = Class.forName("simpledb.QueryPlanVisualizer");
                m = c.getMethod(
                        "printQueryPlanTree", DbIterator.class, System.out.getClass());
                m.invoke(c.newInstance(), physicalPlan,System.out);
            } catch (ClassNotFoundException | SecurityException ignored) {
            } catch (NoSuchMethodException | IllegalArgumentException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }

        return query;
    }

    public Query handleInsertStatement(ZInsert zInsert, TransactionId tId)
            throws TransactionAbortedException, DbException, IOException,
            mydb.ParsingException, Zql.ParseException {
        int tableId;
        try {
            tableId = Database.getCatalog().getTableId(zInsert.getTable()); // will
                                                                      // fall
            // through if
            // table
            // doesn't
            // exist
        } catch (NoSuchElementException e) {
            throw new mydb.ParsingException("Unknown table : "
                    + zInsert.getTable());
        }

        TupleDetail td = Database.getCatalog().getTupleDetail(tableId);

        Tuple t = new Tuple(td);
        int i = 0;
        DbIterator newTups;

        if (zInsert.getValues() != null) {
            @SuppressWarnings("unchecked")
            Vector<ZExp> values = (Vector<ZExp>) zInsert.getValues();
            if (td.fieldNumber() != values.size()) {
                throw new mydb.ParsingException(
                        "INSERT statement does not contain same number of fields as table "
                                + zInsert.getTable());
            }
            for (ZExp e : values) {

                if (!(e instanceof ZConstant))
                    throw new mydb.ParsingException(
                            "Complex expressions not allowed in INSERT statements.");
                ZConstant zc = (ZConstant) e;
                if (zc.getType() == ZConstant.NUMBER) {
                    if (td.getFieldType(i) != Type.INT_TYPE) {
                        throw new mydb.ParsingException("Value "
                                + zc.getValue()
                                + " is not an integer, expected a string.");
                    }
                    IntField f = new IntField(new Integer(zc.getValue()));
                    t.setField(i, f);
                } else if (zc.getType() == ZConstant.STRING) {
                    if (td.getFieldType(i) != Type.STRING_TYPE) {
                        throw new mydb.ParsingException("Value "
                                + zc.getValue()
                                + " is a string, expected an integer.");
                    }
                    StringField f = new StringField(zc.getValue(),
                            Type.STRING_LEN);
                    t.setField(i, f);
                } else {
                    throw new mydb.ParsingException(
                            "Only string or int fields are supported.");
                }

                i++;
            }
            ArrayList<Tuple> tups = new ArrayList<>();
            tups.add(t);
            newTups = new TupleArrayIterator(tups);

        } else {
            ZQuery zq = zInsert.getQuery();
            LogicalPlan lp = parseQueryLogicalPlan(tId, zq);
            newTups = lp.physicalPlan(tId, TableStats.getStatsMap(), explain);
        }
        Query insertQ = new Query(tId);
        insertQ.setPhysicalPlan(new Insert(tId, newTups, tableId));
        return insertQ;
    }

    public Query handleDeleteStatement(ZDelete zDelete, TransactionId tid)
            throws TransactionAbortedException, DbException, IOException,
            mydb.ParsingException, Zql.ParseException {
        int id;
        try {
            id = Database.getCatalog().getTableId(zDelete.getTable()); // will fall
                                                                 // through if
                                                                 // table
                                                                 // doesn't
                                                                 // exist
        } catch (NoSuchElementException e) {
            throw new mydb.ParsingException("Unknown table : "
                    + zDelete.getTable());
        }
        String name = zDelete.getTable();
        Query sdbq = new Query(tid);

        LogicalPlan lp = new LogicalPlan();
        lp.setQuery(zDelete.toString());

        lp.addScan(id, name);
        if (zDelete.getWhere() != null)
            processExpression(tid, (ZExpression) zDelete.getWhere(), lp);
        lp.addProjectField("null.*", null);

        DbIterator op = new Delete(tid, lp.physicalPlan(tid,
                TableStats.getStatsMap(), false));
        sdbq.setPhysicalPlan(op);

        return sdbq;

    }

    // TODO : delete? tranction didn't show in the code
    public void handleTransactStatement(ZTransactStmt s)
            throws TransactionAbortedException, DbException, IOException,
            mydb.ParsingException, Zql.ParseException {
        switch (s.getStmtType()) {
            case "COMMIT":
                if (curtrans == null)
                    throw new ParsingException("No transaction is currently running");
                curtrans.commit();
                curtrans = null;
                inUserTrans = false;
                System.out.println("Transaction " + curtrans.getId().getId()
                        + " committed.");
                break;
            case "ROLLBACK":
                if (curtrans == null)
                    throw new ParsingException( "No transaction is currently running");
                curtrans.abort();
                curtrans = null;
                inUserTrans = false;
                System.out.println("Transaction " + curtrans.getId().getId()
                        + " aborted.");

                break;
            case "SET TRANSACTION":
                if (curtrans != null)
                    throw new ParsingException("Can't start new transactions " +
                            "until current transaction has been committed or rolledback.");
                curtrans = new Transaction();
                curtrans.start();
                inUserTrans = true;
                System.out.println("Started a new transaction tid = "
                        + curtrans.getId().getId());
                break;
            default:
                throw new ParsingException("Unsupported operation");
        }
    }

    public LogicalPlan generateLogicalPlan(TransactionId tid, String s)
            throws mydb.ParsingException {
        ByteArrayInputStream bis = new ByteArrayInputStream(s.getBytes());
        ZqlParser p = new ZqlParser(bis);
        try {
            ZStatement stmt = p.readStatement();
            if (stmt instanceof ZQuery) {
                LogicalPlan lp = parseQueryLogicalPlan(tid, (ZQuery) stmt);
                return lp;
            }
        } catch (Zql.ParseException e) {
            throw new mydb.ParsingException(
                    "Invalid SQL expression: \n \t " + e);
        } catch (IOException e) {
            throw new mydb.ParsingException(e);
        }

        throw new mydb.ParsingException(
                "Cannot generate logical plan for expression : " + s);
    }

    public void setTransaction(Transaction t) {
        curtrans = t;
    }

    public Transaction getTransaction() {
        return curtrans;
    }

    public void processNextStatement(String s) {
        processNextStatement(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
    }

    public void processNextStatement(InputStream is) {

        try{
            ZqlParser p = new ZqlParser(is);
            ZStatement s = p.readStatement();

            Query query = null;
            if (s instanceof ZTransactStmt)
                //TODO : COMMIT / ROLLBACK
                handleTransactStatement((ZTransactStmt) s);
            else {
                if (!inUserTrans) {
                    // a transaction and manages transaction commit / abort.
                    curtrans = new Transaction();
                    curtrans.start();
                    System.out.println("Started a new transaction tid = "
                            + curtrans.getId().getId());
                }
                try{
                    if(s instanceof ZInsert)
                        query = handleInsertStatement((ZInsert) s,
                                curtrans.getId());
                    else if (s instanceof ZDelete)
                        query = handleDeleteStatement((ZDelete) s,
                                curtrans.getId());
                    else if (s instanceof ZQuery)
                        query = handleQueryStatement((ZQuery) s,
                                curtrans.getId());
                    else{
                        System.out.println("Can't parse "
                                        + s
                                        + "\n -- parser only handles SQL transactions, insert, delete, and select statements");
                    }
                    if (query != null)
                        query.execute();

                    if (!inUserTrans && curtrans != null) {
                        curtrans.commit();
                        System.out.println("Transaction "
                                + curtrans.getId().getId() + " committed.");
                    }
                } catch (Throwable a) {
                    // Whenever error happens, abort the current transaction
                    if (curtrans != null) {
                        curtrans.abort();
                        System.out.println("Transaction "
                                + curtrans.getId().getId()
                                + " aborted because of unhandled error");
                    }
                    this.inUserTrans = false;

                    if (a instanceof mydb.ParsingException
                            || a instanceof Zql.ParseException)
                        throw new ParsingException((Exception) a);
                    if (a instanceof Zql.TokenMgrError)
                        throw (Zql.TokenMgrError) a;
                    throw new DbException(a.getMessage());
                } finally {
                    if (!inUserTrans)
                        curtrans = null;
                }
            }

        } catch (TransactionAbortedException | IOException | DbException e) {
            e.printStackTrace();
        } catch (mydb.ParsingException e) {
            System.out.println("Invalid SQL expression: \n \t" + e.getMessage());
        } catch (ParseException | TokenMgrError e) {
            System.out.println("Invalid SQL expression: \n \t " + e);
        }
    }

    // Basic SQL completions
    public static final String[] SQL_COMMANDS = { "select", "from", "where",
            "group by", "max(", "min(", "avg(", "count", "rollback", "commit",
            "insert", "delete", "values", "into" };

    public static void main(String[] argv) throws IOException {

        if (argv.length < 1 || argv.length > 4) {
            System.out.println("Invalid number of arguments.\n" + usage);
            System.exit(0);
        }

        Parser p = new Parser();

        p.start(argv);
    }

    static final String usage = "Usage: parser catalogFile [-explain] [-f queryFile]";

    protected void shutdown() {
        System.out.println("Bye");
    }

    protected boolean interactive = true;

    protected void start(String[] argv) throws IOException {
        // first add tables to database
        Database.getCatalog().loadSchema(argv[0]);
        // TODO: class TableStats  delete?
        TableStats.computeStatistics();

        String queryFile = null;



        // TODO: explain part delete
        if (argv.length > 1) {
            for (int i = 1; i < argv.length; i++) {
                if (argv[i].equals("-explain")) {
                    explain = true;
                    System.out.println("Explain mode enabled.");
                } else if (argv[i].equals("-f")) {
                    interactive = false;
                    if (i++ == argv.length) {
                        System.out.println("Expected file name after -f\n"
                                + usage);
                        System.exit(0);
                    }
                    queryFile = argv[i];

                } else {
                    System.out.println("Unknown argument " + argv[i] + "\n "
                            + usage);
                }
            }
        }
        if (!interactive) {
            try {
                // curtrans = new Transaction();
                // curtrans.start();
                long startTime = System.currentTimeMillis();
                assert queryFile != null;
                processNextStatement(new FileInputStream(new File(queryFile)));
                long time = System.currentTimeMillis() - startTime;
                System.out.printf("----------------\n%.2f seconds\n\n",
                        ((double) time / 1000.0));
                System.out.println("Press Enter to exit");
                System.in.read();
                this.shutdown();
            } catch (FileNotFoundException e) {
                System.out.println("Unable to find query file" + queryFile);
                e.printStackTrace();
            }
        } else { // no query file, run interactive prompt
            ConsoleReader reader = new ConsoleReader();

            // Add really stupid tab completion for simple SQL
            ArgumentCompletor completor = new ArgumentCompletor(
                    new SimpleCompletor(SQL_COMMANDS));
            completor.setStrict(false); // match at any position
            reader.addCompletor(completor);

            StringBuilder buffer = new StringBuilder();
            String line;
            boolean quit = false;
            while (!quit && (line = reader.readLine("DB-> ")) != null) {
                // Split statements at ';': handles multiple statements on one
                // line, or one
                // statement spread across many lines
                while (line.indexOf(';') >= 0) {
                    int split = line.indexOf(';');
                    buffer.append(line, 0, split + 1);
                    // find what the cmd is
                    String cmd = buffer.toString().trim();
                    cmd = cmd.substring(0, cmd.length() - 1).trim() + ";";
                    byte[] statementBytes = cmd.getBytes(StandardCharsets.UTF_8);
                    if (cmd.equalsIgnoreCase("quit;")
                            || cmd.equalsIgnoreCase("exit;")) {
                        shutdown();
                        quit = true;
                        break;
                    }

                    long startTime = System.currentTimeMillis();
                    // start sql process
                    processNextStatement(new ByteArrayInputStream(statementBytes));
                    long time = System.currentTimeMillis() - startTime;
                    System.out.printf("----------------\n%.2f seconds\n\n",
                            ((double) time / 1000.0));

                    // Grab the remainder of the line
                    line = line.substring(split + 1);
                    buffer = new StringBuilder();
                }
                if (line.length() > 0) {
                    buffer.append(line);
                    buffer.append("\n");
                }
            }
        }
    }
}

class TupleArrayIterator implements DbIterator {
    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;
    ArrayList<Tuple> tups;
    Iterator<Tuple> it = null;

    public TupleArrayIterator(ArrayList<Tuple> tups) {
        this.tups = tups;
    }

    public void open() throws DbException, TransactionAbortedException {
        it = tups.iterator();
    }

    /** @return true if the iterator has more items. */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return it.hasNext();
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     * 
     * @return The next tuple in the iterator, or null if there are no more
     *         tuples.
     */
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        return it.next();
    }

    /**
     * Resets the iterator to the start.
     * 
     * @throws DbException
     *             When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
        it = tups.iterator();
    }

    /**
     * Returns the TupleDesc associated with this DbIterator.
     */
    public TupleDetail getTupleDetail() {
        return tups.get(0).getTupleDetail();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
    }

//    public static void main(String args[]) {
//        System.out.println("args[] = "+ args);
//        for(int i = 0;i< args.length;i++)
//            System.out.println("args["+ i + "] =" + args[i]);
//
//    }

}
