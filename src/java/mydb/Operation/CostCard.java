package mydb.Operation;
import mydb.LogicalJoinNode;

import java.util.Vector;

// specifying the cost and cardinality of the optimal plan represented by plan
public class CostCard {

    // The cost of the optimal subplan
    public double cost;

    // The cardinality of the optimal subplan
    public int card;

    // The optimal subplan
    public Vector<LogicalJoinNode> plan;
}
