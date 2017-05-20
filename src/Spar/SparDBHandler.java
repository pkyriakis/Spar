package Spar;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

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
	
	
	public void createTables() throws SQLException
	{
		// Set up queries
		String nodesT = "create table " + nodesTable + " (id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY , "
				+ "url TEXT, title TEXT, backlinks INT(6))"; 
		String edgesT = "create table " + edgesTable + " (id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY , "
				+ "start INT(6), end INT(6), label VARCHAR(30))"; 
		
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
	
	public Vector<Node> loadNodesTable() throws SQLException
	{
		// Set up query and statement
		String query = "select * from " + nodesTable + " order by id ASC";
		PreparedStatement statement = con.prepareStatement(query);
		
		// Execute statement and get resultset
		statement.execute();
		ResultSet res = statement.getResultSet();
		
		// Result containing result
		Vector<Node> nodes = new Vector<Node>();
		while(res.next())
			nodes.add(new Node(res.getInt("id"),res.getString("url"), 
								res.getString("title"), res.getInt("backlinks")));
			
		// Close
		statement.close();
		res.close();
		
		return nodes;
	}
	
	/**
	 * Load the edge table; return a vector of nodes
	 * @throws SQLException 
	 * */
	
	public Vector<Edge> loadEdgesTable() throws SQLException
	{
		// Set up query and statement
		String query = "select * from " + edgesTable + " order by id ASC";
		PreparedStatement statement = con.prepareStatement(query);
		
		// Execute statement and get resultset
		statement.execute();
		ResultSet res = statement.getResultSet();
		
		// Result containing result
		Vector<Edge> edges = new Vector<Edge>();
		while(res.next())
			edges.add(new Edge(res.getInt("id"),res.getInt("start"), 
								res.getInt("end"), res.getString("label")));
			
		// Close
		statement.close();
		res.close();
		
		return edges;
	}
	
	/**
	 * Put data into nodes table
	 * @throws SQLException 
	 **/	
	public void putInNodesTable(Node node) throws SQLException 
	{		
		// Set up query
		String query = String.format("insert into %s (url,title,backlinks) values (?,?,?);",nodesTable);
		PreparedStatement statement = con.prepareStatement(query);
	
		// Insert Row data
		statement.setString(1, node.url);
		statement.setString(2, node.title);
		statement.setInt(3, node.backLinks);
		
		// Execute and close
		statement.executeUpdate();
		statement.close();
		return;
	}
	
	/**
	 * Put data into nodes table
	 * @throws SQLException 
	 **/	
	public void putInEdgesTable(Edge edge) throws SQLException 
	{		
		// Set up query
		String query = String.format("insert into %s (start,end,label) values (?,?,?);",nodesTable);
		PreparedStatement statement = con.prepareStatement(query);
	
		// Insert Row data
		statement.setInt(1, edge.start);
		statement.setInt(2, edge.end);
		statement.setString(3, edge.label);
		
		// Execute and close
		statement.executeUpdate();
		statement.close();
		return;
	}
	
}
