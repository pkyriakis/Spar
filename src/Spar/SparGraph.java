package Spar;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import Spar.SparDBHandler;
import Spar.SparGraph.Node;


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
		public Integer vbackLinks=0;
		public boolean scanned;
		
		
		Node(Integer id, String url, String title, Integer backLinks){
			this.id = id;
			this.url = url;
			this.backLinks = backLinks;
			this.title = title;
		}
		
		void setVBackLinks(int vBL)
		{
			this.vbackLinks = vBL;
		}
		void setScanned()
		{
			this.scanned = true;
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
	
	// Maps urls to Nodes; easy to getNodeByUrl
	HashMap<String, Node> urlNodeMap = new HashMap<String, Node>();
	
	// Keep track of the indices of last node and edge added in DB
	private Integer indOfLastNode = 0;
	private Integer indOfLastEdge = 0;
	
	// Node's whose virtual BL have been updates; used for avoiding cycles in the recursion of constructVBackLinks()
	private HashSet<Node> nodesVBLDone = new HashSet<Node>();
	
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
		urlNodeMap.put(n.url, n);
		return;
	}
	
	public void addEdge(Edge e)
	{
		Edges.add(e);
		return;
	}
	
	/**
	 * Add edge labeled by label from start to end nodes
	 * */
	public void addEdge(Node start, Node end, String label)
	{
		// Id of new edge
		Integer ind = Edges.size() + 1;
		
		// Create it
		Edge e = new Edge(ind, start.id, end.id, label);
		
		// Add
		addEdge(e);
		
		return;
	}
	
	
	/**
	 * Create a new node for linkHref and add an Edge labeled with label between this and start
	 * */
	public void addNodeEdge(Node start, String linkHref, String label)
	{
		  // Create node for linkHref; title empty for now
		  Integer nodeHrefId = getNodesSize()+1;
		  Node nodeHref = new Node(nodeHrefId,linkHref,"",0);
		  addNode(nodeHref);
		  
		  // Create edge url->linkHref
		  Integer edgeUrlHrefId = getEdgesSize() + 1;
		  Edge edgeUrlHref = new Edge(edgeUrlHrefId, start.id, nodeHrefId, label);
		  addEdge(edgeUrlHref);

		  return;
	}
	
	/**
	 * Increase the backlink count of node id
	 * */
	public void increaseBackLinksOf(Integer nodeId)
	{
		Node n = getNodeById(nodeId);
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
	public Node getNodeById (Integer id)
	{
		// Note: Vector start indexing at 0; db starts at 1
		return Nodes.elementAt(id - 1);
	}
	
	/**
	 * Return a Edge filtered by id
	 */
	public Edge getEdgeById (Integer id)
	{
		// Note: Vector start indexing at 0; db starts at 1
		return Edges.elementAt(id - 1);
	}
	
	
	/**
	 * Return Node of url
	 */
	public Node getNodeByUrl(String url) 
	{		
		// Find the node of given url; null if none
		return urlNodeMap.get(url);
	}
	
	/**
	 * Return outgoing edges of Node n
	 * */
	public Vector<Edge> getOutEdgesOf(Node n)
	{
		Vector<Edge> out = new Vector<Edge>();
		for(Edge e : Edges)
			if(e.start.equals(n.id))
				out.add(e);
		
		return out;
	}
	
	/**
	 * Return ingoing edges of nodeId
	 * */
	public Vector<Edge> getInEdgesOf(Node n)
	{
		Vector<Edge> out = new Vector<Edge>();
		for(Edge e : Edges)
			if(e.end.equals(n.id))
				out.add(e);
		
		return out;
	}
	
	
	/**
	 * Get the set of nodes that have a directed path ending at end 
	 * */
	public HashSet<Node> getDPstartingNodes(Node end)
	{		
		// Pending and done lists
		Vector<Node> queue = new Vector<Node>();
		HashSet<Node> visited = new HashSet<Node>();
		
		// Add end node to pending
		queue.add(end);
		
		Node head;
		Integer i = 0;	
		
		// Loop over queue
		while(i < queue.size())
		{		
			// Get head
			head = queue.get(i);
			// Add head to visited
			visited.add(head);
			// Get in edges 
			Vector<Edge> in = getInEdgesOf(head);

			//Loop over them
			for(Edge e : in){
				// Add those not already visited
				Node parent = getNodeById(e.start);
				if(!visited.contains(parent))
					queue.add(parent);
			}
			i++;
		}
		
		return visited;
	}
	
	/**
	 * Transfer new Nodes and Edges to DB and update previous nodes (cuz of new backlinks and vbacklinks)
	 * @throws SQLException 
	 * */
	public void transferToDB(SparDBHandler dbHandler) throws SQLException
	{
		// Make sure virtual backlinks are update before putting in db
		//nodesVBLDone = new HashSet<Node>();
		//constructVBackLinks(Nodes.firstElement());
		
		// Update existing nodes (new v/backlinks count)
		Integer i=0;
		for(i=0; i < indOfLastNode; i++)
			dbHandler.updateNodesTable(Nodes.elementAt(i));
		
		// Put new nodes
		while(indOfLastNode < Nodes.size()){
			dbHandler.putInNodesTable(Nodes.get(indOfLastNode));
			indOfLastNode++;
		}
			
		// Put new edges
		while(indOfLastEdge < Edges.size()){
			dbHandler.putInEdgesTable(Edges.get(indOfLastEdge));
			indOfLastEdge++;
		}
		
		return;
	}
	
	/**
	 * Returns the children of the given node
	 * */
	public Vector<Node> getChildrenOf (Node n)
	{
		Vector<Node> children = new Vector<Node>();
		
		// Get outgoing edges of node
		Vector<Edge> out = getOutEdgesOf(n);
		
		// Scan them and add their end nodes to children vector
		for(Edge e : out)
			children.add(getNodeById(e.end));
		
		return children;
		
	}
	
	
	/**
	 * Returns the node that was last scanned in a previous run
	 * */
	public Node lastScanned()
	{
		Node last = Nodes.firstElement(); // worst case it's the first
		Integer i = 0;
		boolean found = false;
		
		while(i < getNodesSize() && !found){
			if(!Nodes.get(i).scanned){
				last = Nodes.get(i-1);
				found = true;
			}
			i++;
		}
		return last;
	}
	
	/**
	 * Calculates the virtual backlinks of nodes; 
	 * 
	 * For a node u having children Cu, the vBackLinks are defined as max(BL(u),max(BL(C(u))))
	 * */
	public void constructVBackLinks(Node parent)
	{
		// Init parent's backlinks count; only needed for the zero-th level recursion
		parent.setVBackLinks(parent.backLinks);
				
		// Get children
		Vector<Node> children = getChildrenOf(parent);
		
		if(children.isEmpty() || nodesVBLDone.contains(parent))// has reached leaves or parent's vBL have been updated; avoids cycles
			return;
		
		nodesVBLDone.add(parent);
		
		for(Node child : children)
		{
			// Init child's VbackLinks to its own backlinks 
			child.setVBackLinks(child.backLinks);
			
			// Construct its vBackLinks; recursively
			constructVBackLinks(child);
						
			// Compare to parent's backlinks
			if(child.vbackLinks > parent.vbackLinks)
				parent.vbackLinks = child.vbackLinks;
			
		}	
	}				

}
