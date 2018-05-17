package Spar;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import Spar.SparDBHandler;


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
		public String html;
		public boolean scanned = false;
		
		// Nodes in the following set create a Directed path ending at current instance of Node
		// Store ids not node class
		HashSet<Integer> DPFrom = new HashSet<Integer>();
		
		
		Node(Integer id, String url, String title, Integer backLinks){
			this.id = id;
			this.url = url;
			this.backLinks = backLinks;
			this.title = title;
		}
		@Override
		public String toString()
		{
			return "id = " + this.id + " url = " + this.url + " backlinks = " + this.backLinks;
		}
		
		// Converts the DPFrom set to a string
		public String dpFromToString()
		{
			return this.DPFrom.toString();
		}
		
		// Creates the DPFrom from the given string 
		public void setDPFrom (String s)
		{
			if(s.equals("[]"))
				return;
			
			String[] subs;
			if(s.contains(", "))
			{
				subs = s.replace("[", "").replace("]", "").split(", ");
				for(String ss : subs)
					this.DPFrom.add(Integer.parseInt(ss));
			}
			else
			{
				this.DPFrom.add(Integer.parseInt(s.replace("[", "").replace("]", "")));
			} 
			
			return;
		}
		
		void setVBackLinks(int vBL)
		{
			this.vbackLinks = vBL;
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
		
		public String toString()
		{
			return "id = " + this.id + " start = " + this.start + " end = " + this.end + " label = "+ this.label;
		}
		
		public String endLabelCode()
		{
			return this.label+this.end;
		}
	}
		

	
	/**
	 * Vectors of nodes and edges; represents the structure of the graph;  
	 * */
	private Vector<Node> Nodes;
	private Vector<Edge> Edges;
	
	// Maps urls to Nodes; easy to getNodeByUrl
	private HashMap<String, Node> urlNodeMap = new HashMap<String, Node>();
	
	// Keeps track of the codes of added edges; to make sure that no edges with same end node and label are added
	private HashSet<String> endLabelCodes = new HashSet<String>();
	
		
	// Keep track of the indices of last node and edge added in DB
	private Integer indOfLastNode = 0;
	private Integer indOfLastEdge = 0;
		
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
	
	public synchronized Vector<Node> nodeSet()
	{
		return this.Nodes;
	}

	public synchronized Vector<Edge> edgeSet()
	{
		return this.Edges;
	}
	public synchronized void addNode(Node n)
	{
		// Add node to graph
		Nodes.add(n);
		
		// Update the urlNode map
		urlNodeMap.put(n.url, n);

		return;
	}
	
	
	/**
	 * Checks if the an edge labeled by label to node end exists  
	 * */
	public synchronized boolean existsEndLabel(Node end, String label)
	{
		// Code = label+end		
		if(endLabelCodes.contains(label+end.id)){
			return true;
		}
		
		return false;
	}
	
	
	/**
	 * Adds an edge to graph and saves its endLabelCode
	 * */
	public synchronized void addEdge(Edge e)
	{		
		// Add it
		Edges.add(e);
		
		// Save its code
		endLabelCodes.add(e.endLabelCode());
		return;
	}
	
	/**
	 * Add edge labeled by label from start to end nodes
	 * */
	public synchronized void addEdge(Node start, Node end, String label)
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
	 * 
	 * */
	public synchronized void setScanned(Node n)
	{
		getNodeById(n.id).scanned = true;
	}
	
	
	/**
	 * Create a new node for linkHref and add an Edge labeled with label between this and start; Return the added node
	 * */
	public synchronized Node addNodeEdge(Node start, String linkHref, String label)
	{
		  // Create node for linkHref; title empty for now
		  Integer nodeHrefId = getNodesSize()+1;
		  Node nodeHref = new Node(nodeHrefId,linkHref,"",0);
		  addNode(nodeHref);
		  
		  // Create edge url->linkHref
		  Integer edgeUrlHrefId = getEdgesSize() + 1;
		  Edge edgeUrlHref = new Edge(edgeUrlHrefId, start.id, nodeHrefId, label);
		  addEdge(edgeUrlHref);

		  return nodeHref;
	}
	
	/**
	 * Increase the backlink count of node id
	 * */
	public synchronized void increaseBackLinksOf(Node node)
	{
		node.backLinks++;
		return;
	}
	
	
	/**
	 * Return the size of the Nodes and Edges vector
	 * */
	public synchronized Integer getNodesSize()
	{
		return Nodes.size();
	}

	public synchronized Integer getEdgesSize()
	{
		return Edges.size();
	}
	
	/**
	 * Return a Node filtered by id
	 */
	public synchronized Node getNodeById (Integer id)
	{
		// Note: Vector start indexing at 0; db starts at 1
		if (id <= getNodesSize())
			return Nodes.elementAt(id - 1);
		else
			return null;
	}
	
	/**
	 * Return a Edge filtered by id
	 */
	public synchronized Edge getEdgeById (Integer id)
	{
		// Note: Vector start indexing at 0; db starts at 1
		return Edges.elementAt(id - 1);
	}
	
	
	/**
	 * Return Node of url
	 */
	public synchronized Node getNodeByUrl(String url) 
	{		
		// Find the node of given url; null if none
		return urlNodeMap.get(url);
	}
	
	/**
	 * Return outgoing edges of Node n
	 * */
	public synchronized Vector<Edge> getOutEdgesOf(Node n)
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
	public synchronized Vector<Edge> getInEdgesOf(Node n)
	{
		Vector<Edge> out = new Vector<Edge>();
		for(Edge e : Edges)
			if(e.end.equals(n.id))
				out.add(e);
		
		return out;
	}
	
	/**
	 * Returns the edge labels on directed paths ending on end
	 * */
	public synchronized HashSet<String> traceBackLabels(Node end)
	{
		// Pending and done lists
		Vector<Node> queue = new Vector<Node>();
		HashSet<Node> visited = new HashSet<Node>();
		
		HashSet<String> labels = new HashSet<String>();
		
		// Add end node to pending
		queue.add(end);
		
		Node head;
		Integer i = 0;	
		
		// Loop over queue
		while(i < queue.size())
		{		
			// Get head
			head = queue.get(i);
			System.out.println(head.url);

			// Add head to visited
			visited.add(head);
			// Get in edges 
			Vector<Edge> in = getInEdgesOf(head);

			//Loop over them
			for(Edge e : in){
				// Get parent
				Node parent = getNodeById(e.start);

				// Add label; temp, adds BL too
				labels.add(e.label+parent.backLinks);
				
				// Add those not already visited
				if(!visited.contains(parent) && !labels.contains(e.label))
					queue.add(parent);
			}
			i++;
		}
		
		return labels;
	}
	
	
	/**
	 * Stores the ids of nodes that have a directed path ending at end in end.DPFrom; need to store ids cuz they
	 * are needed when loading the graph 
	 * 
	 * Depreciated; essentially BFS following reverse edge directions; too slow to run it every time a new node is scanned
	 * */
	public synchronized void getDPstartingNodes(Node end)
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
			// Store its id
			end.DPFrom.add(head.id);
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
		
		return;
	}
	
	
	/**
	 * Sets the set end.PDFrom; recursively
	 * */
	public synchronized void setDPFrom(Node end)
	{
		// Node has DP from itself; trivial
		end.DPFrom.add(end.id);
		
		// Get end's parents
		Vector<Node> parents = getParentsOf(end);
		
		// Add p.DPFrom to end.DPFrom for all parents p 
		for(Node p : parents)
			end.DPFrom.addAll(p.DPFrom);		
		
		return;
	}
	
	
	/**
	 * Returns the parents of n
	 * */
	public synchronized Vector<Node> getParentsOf (Node n)
	{
		Vector<Node> parents = new Vector<Node>();
		Vector<Edge> in = getInEdgesOf(n);
		for(Edge e : in)
			parents.add(getNodeById(e.start));		
		return parents;
	}
	
	public synchronized boolean existsDP (Node start, Node end)
	{		
		// Check if start is in there
		return getNodeById(end.id).DPFrom.contains(start.id);
	}
	
	/**
	 * Transfer new Nodes and Edges to DB and update previous nodes (cuz of new backlinks and vbacklinks)
	 * @throws SQLException 
	 * */
	public synchronized void transferToDB(SparDBHandler dbHandler) throws SQLException
	{
		// Make sure virtual backlinks are update before putting in db
		//nodesVBLDone = new HashSet<Node>();
		//constructVBackLinks(Nodes.firstElement());		
		
		// Update existing nodes (new v/backlinks count)
		dbHandler.updateNodesTable(Nodes.subList(0, indOfLastNode));
		
		// Put new nodes
		dbHandler.putInNodesTable(Nodes.subList(indOfLastNode, Nodes.size()));
	
		// Put new edges
		dbHandler.putInEdgesTable(Edges.subList(indOfLastEdge, Edges.size()));
		
		// Update counters
		indOfLastNode = Nodes.size();
		indOfLastEdge = Edges.size();

		return;
	}
	
	/**
	 * Returns the children of the given node
	 * */
	public synchronized Vector<Node> getChildrenOf (Node n)
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
	 * Returns the children of the given node
	 * */
	public synchronized Vector<String> getLabelsToChildrenOf (Node n)
	{
		Vector<String> labels = new Vector<String>();
		
		// Get outgoing edges of node
		Vector<Edge> out = getOutEdgesOf(n);
		
		// Scan them and add their end nodes to children vector
		for(Edge e : out)
			labels.add(e.label);
		
		return labels;
		
	}
	
	/**
	 * Returns the node that was last scanned in a previous run
	 * */
	public synchronized Node firstNotScanned()
	{
		Node first = Nodes.lastElement(); // worst case when size=1

		Integer i = Nodes.size() - 1;
		boolean found = false;
		
		while(i >= 0 && !found){
			if(Nodes.get(i).scanned){
				first = Nodes.get(i+1);
				found = true;
			}
			i--;
		}

		return first;
	}
	
	/**
	 * Increases the virtual backlinks of nodes that have a directed path to the given nid; 
	 * There is a BL B->A if there is a directed path A->B 
	 * */
	public synchronized void increaseVBackLinksOf(Integer nid)
	{
		// Increase vBL of nid 
		getNodeById(nid).vbackLinks = getNodeById(nid).vbackLinks + 1;

		
	}

	public synchronized Integer getIndOfLastNode() {
		return indOfLastNode;
	}

	public synchronized void setIndOfLastNode(Integer indOfLastNode) {
		this.indOfLastNode = indOfLastNode;
	}

	public synchronized Integer getIndOfLastEdge() {
		return indOfLastEdge;
	}

	public synchronized void setIndOfLastEdge(Integer indOfLastEdge) {
		this.indOfLastEdge = indOfLastEdge;
	}				

}
