package Spar;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import Spar.SparGraph.*;


/**
 * Handles connection to database as well as input/output 
 * */

public class SparDBHandler {
	
	// Init null connection
	private Connection con = null;
	
	// Table name
	private String nodesTable;
	private String edgesTable;
	
	// 
	private Integer Batch_Size = 10000;
	
	public SparDBHandler(String nodesTable, String edgesTable) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
	{
		//Load Driver
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		 // Setup the connection with the DB
        con = DriverManager
                .getConnection("jdbc:mysql://localhost/spar?"
                        + "user=root&password=");
        
        this.nodesTable = nodesTable;
        this.edgesTable = edgesTable;
        return;
	}
	
	/**
	 * Closes connection to DB
	 * @throws SQLException 
	 * */
	public void close() throws SQLException
	{
		con.close();
		return;
	}
	
	/**
	 * Creates tables for nodes and edges, if they do not exist, as given by field names.
	 * @throws SQLExpection
	 * */
	public void createTables() throws SQLException
	{
		// Set up queries
		String nodesT = "create table if not exists " + nodesTable + " (id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY , "
				+ "url LONGTEXT, title LONGTEXT, backlinks INT(6), vbacklinks INT(6), scanned BOOLEAN, dpfrom LONGTEXT)"; 
		String edgesT = "create table if not exists " + edgesTable + " (id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY , "
				+ "start INT(6), end INT(6), label LONGTEXT)"; 
		
		// Run them
		PreparedStatement statementN = con.prepareStatement(nodesT);
		statementN.execute();
		statementN.close();
		
		PreparedStatement statementE = con.prepareStatement(edgesT);
		statementE.execute();
		statementE.close();

		return;
	}
	

	/**
	 * Drops nodes and edges table
	 * @throws SQLException 
	 * */
	public void dropTables() throws SQLException
	{		
		// Set up queries
		String nodesT = "drop table " + nodesTable;
		String edgesT = "drop table " + edgesTable;
		
		// Run them
		PreparedStatement statementN = con.prepareStatement(nodesT);
		statementN.execute();
		statementN.close();
		
		PreparedStatement statementE = con.prepareStatement(edgesT);
		statementE.execute();
		statementE.close();
		return;
	}

	/**
	 * Load the node table; return a vector of nodes
	 * @throws SQLException 
	 * */
	
	public SparGraph loadGraph() throws SQLException
	{
		SparGraph Graph = new SparGraph();
		
		// Set up query and statement for nodes
		String query = "select * from " + nodesTable + " order by id ASC";
		PreparedStatement statement = con.prepareStatement(query);
		
		// Execute statement and get resultset
		statement.execute();
		ResultSet res = statement.getResultSet();
		
		// Loop over result set
		while(res.next()){
			Node newNode = new Node(res.getInt("id"),res.getString("url"), 
					res.getString("title"), res.getInt("backlinks"));
			
			newNode.setVBackLinks(res.getInt("vbacklinks"));
			if (res.getBoolean("scanned") == true)
				newNode.scanned = true;
			newNode.setDPFrom(res.getString("dpfrom"));
			
			Graph.addNode(newNode);
		}
		// Close
		statement.close();
		res.close();
		
		// Set up query and statement for edges
		query = "select * from " + edgesTable + " order by id ASC";
		statement = con.prepareStatement(query);
		
		// Execute statement and get resultset
		statement.execute();
		res = statement.getResultSet();
		
		// Loop over result set
		while(res.next())
		{
			// Grap edge's data
			Node start = Graph.getNodeById(res.getInt("start"));
			Node end = Graph.getNodeById(res.getInt("end"));
			String label = res.getString("label");
			
			
			// Add it to graph
			Graph.addEdge(start,end,label);
		}
			
		// Close
		statement.close();
		res.close();
		
		// Update the DPFrom set for all nodes in Graph
		//Graph.updateDPFromOnLoad();
		
		return Graph;
	}
	
	/**
	 * Put data into nodes table
	 * @throws SQLException 
	 **/	
	public void putInNodesTable(List<Node> nodes) throws SQLException 
	{		
		// Set up query
		String query = String.format("insert into %s (url,title,backlinks,vbacklinks,scanned, dpfrom) values (?,?,?,?,?,?);",nodesTable);
		PreparedStatement statement = con.prepareStatement(query);
	
		Integer cnt = 0;
		
		// Insert Nodes' data into batch
		for(Node node : nodes)
		{
			statement.setString(1, node.url);
			statement.setString(2, node.title);
			statement.setInt(3, node.backLinks);
			statement.setInt(4, node.vbackLinks);
			statement.setBoolean(5, node.scanned);
			statement.setString(6, node.dpFromToString());
			statement.addBatch();
			
			if(++cnt % Batch_Size == 0)
			{
				statement.executeBatch();
				statement.clearBatch();
			}
		}
		
		// Execute last batch
		statement.executeBatch();
		statement.close();
		return;
	}
	
	/**
	 * Put data into edges table
	 * @throws SQLException 
	 **/	
	public void putInEdgesTable(List<Edge> edges) throws SQLException 
	{		
		// Set up query
		String query = String.format("insert into %s (start,end,label) values (?,?,?);",edgesTable);
		PreparedStatement statement = con.prepareStatement(query);
	
		Integer cnt=0;
		
		// Insert Edges' data into batch
		for(Edge edge : edges)
		{
			statement.setInt(1, edge.start);
			statement.setInt(2, edge.end);
			statement.setString(3, edge.label);
			statement.addBatch();
			
			if(++cnt % Batch_Size == 0)
			{
				statement.executeBatch();
				statement.clearBatch();
			}
		}
		
		// Execute last batch
		statement.executeBatch();
		statement.close();
		return;
	}
	
	/**
	 * Put data into nodes table
	 * @throws SQLException 
	 **/	
	public void updateNodesTable(List<Node> nodes) throws SQLException 
	{		
		// Set up query
		String query = String.format("update %s set backlinks=?, vbacklinks=?, scanned=?, dpfrom=? where id=?",nodesTable);
		PreparedStatement statement = con.prepareStatement(query);
	
		Integer cnt = 0;
		
		// Insert Nodes' data into batch
		for(Node node : nodes)
		{
			statement.setInt(1, node.backLinks);
			statement.setInt(2, node.vbackLinks);
			statement.setBoolean(3, node.scanned);
			statement.setString(4, node.dpFromToString());
			statement.setInt(5, node.id);
			statement.addBatch();
			
			if(++cnt % Batch_Size == 0)
			{
				statement.executeBatch();
				statement.clearBatch();
			}
		}
		
		// Last batch
		statement.executeBatch();
		statement.close();
		return;
	}
	
}
