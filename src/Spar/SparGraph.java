package Spar;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;


/**
 * Performs graph-like operations on the Row table  
 * */
public class SparGraph {
	
	/**
	 * Define the structure of the Node and Edge
	 * */
		
	public static class Node{
		public Integer id;
		public String url;
		public String title;
		public Integer backLinks;		
		
		Node(Integer id, String url, String title, Integer backLinks){
			this.id = id;
			this.url = url;
			this.backLinks = backLinks;
			this.title = title;
		}
	}
		
	public static class Edge{
		public Integer id;
		public Integer start;
		public Integer end;
		public String label;		
			
		Edge(Integer id, Integer start, Integer end, String label){
			this.id = id;
			this.start = start;
			this.end = end;
			this.label = label;
		}
	}
		

	
	/**
	 * Vectors of nodes and edges; represents the structure of the graph;  
	 * */
	private Vector<Node> Nodes;
	private Vector<Edge> Edges;
	
	// Initialize new graph
	SparGraph(){
		this.Nodes = new Vector<Node>();
		this.Edges = new Vector<Edge>();
	}
	
	// Initialize using given nodes and edges
	SparGraph(Vector<Node> nodes, Vector<Edge> edges) {
		this.Nodes=nodes;
		this.Edges=edges;
	}
	
	public void addNode(Node n)
	{
		Nodes.add(n);
		return;
	}
	
	public void addEdge(Edge e)
	{
		Edges.add(e);
		return;
	}
	
	/**
	 * Increase the backlink count of node id
	 * */
	void increaseBackLinksOf(Integer nodeId)
	{
		Node n = getNodeOf(nodeId);
		n.backLinks++;
		Nodes.set(nodeId - 1, n);
		return;
	}
	
	
	/**
	 * Return the size of the Nodes and Edges vector
	 * */
	public Integer getNodesSize()
	{
		return Nodes.size();
	}

	public Integer getEdgesSize()
	{
		return Edges.size();
	}
	
	/**
	 * Return a Node filtered by id
	 */
	public Node getNodeOf (Integer id)
	{
		// Note: Vector start indexing at 0; db starts at 1
		return Nodes.elementAt(id - 1);
	}
	
	/**
	 * Return a Edge filtered by id
	 */
	public Edge getEdgeOf (Integer id)
	{
		// Note: Vector start indexing at 0; db starts at 1
		return Edges.elementAt(id - 1);
	}
	
	
	/**
	 * Return Node of url
	 */
	public Node getNodeOf(String url) 
	{		
		// Find the node of given url
		for(Node n : Nodes)
			if(n.url.equals(url))
				return n;
		
		// Return null if not found
		return null;
	}
	
	/**
	 * Return outgoing edges of nodeId
	 * */
	public Vector<Edge> getOutEdgesOf(Integer nodeId)
	{
		Vector<Edge> out = new Vector<Edge>();
		for(Edge e : Edges)
			if(e.start.equals(nodeId))
				out.add(e);
		
		return out;
	}
	
	/**
	 * Return ingoing edges of nodeId
	 * */
	public Vector<Edge> getInEdgesOf(Integer nodeId)
	{
		Vector<Edge> out = new Vector<Edge>();
		for(Edge e : Edges)
			if(e.end.equals(nodeId))
				out.add(e);
		
		return out;
	}
	
	
	/**
	 * Returns true if a directed path from startUrl to endUrl exists; otherwise false
	 * Using BSF; might not be optimal, random would be better
	 * */
	public boolean existsDirectedPath(String startUrl, String endUrl)
	{
		// Rows left to be visited
		Queue<Node> queue = new LinkedList<Node>();
		queue.add(getNodeOf(startUrl));
		
		// Already visited
		Vector<Node> visited = new Vector<Node>();
		
		Node first;
		while((first = queue.poll()) != null)// run while queue not empty
		{			
			if(first.url.equals(endUrl))// check if reached endUrl; if yes directed cycle exists 
				return true;
			else{// havent reached
				visited.add(first);// add first to visited
				Vector<Edge> outEdges = getOutEdgesOf(first.id);// get outgoing edges
				
				for(Edge e : outEdges){ // add to queue those not visited and not in queue
					Node neighbor = getNodeOf(e.end);
					if(!visited.contains(neighbor) && !queue.contains(neighbor))
						queue.add(neighbor);
				}
			}
			
		}
		
		return false;
	}
					

}
