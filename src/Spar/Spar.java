package Spar;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import Spar.SparGraph.*;
import java.sql.*;

public class Spar {

	// The db handler
	static SparDBHandler db;
	
	// The db data
	static SparGraph Graph;
		
	// Counts the nodes and edges added to graph
	private static Integer addedNodes = 0;
	private static Integer addedEdges = 0;
	
	static String TopLevel;
		
	static PrintWriter log;
	
	
	private static void parseElement(Node head, Elements links) throws FileNotFoundException
	{							
		
		// Set head to scanned
		Graph.setScanned(head);
		
		// Get nodes lying on a DP ending at head; stores them in head.DPFrom
		Graph.setDPFrom(head);
					
		log  = new PrintWriter(new FileOutputStream("tmp.txt",true));
		log.write("Scanning " + head.url+ " " + head.DPFrom.toString() +"\n");
		
		// Keeps track of the links in current page that have been added to graph; to avoid duplicates
		Vector<String> done = new Vector<String>();			
				
		for (Element link : links) {
			  // Get href of current link
			  String linkHref = link.attr("abs:href");

			  // Get text on link
			  String linkText = link.text();			  

			  if(linkText.isEmpty())
				  linkText  = link.attr("title");

			  // Only add links from the same host
			  if((linkHref.contains(TopLevel) || linkHref.contains("bbc.com")) && !linkHref.contains("#") && !done.contains(linkHref))
			  {				  
				  // Add to done links
				  done.add(linkHref);
				  
				  // Get node of Href; if any
				  Node nodeHref = Graph.getNodeByUrl(linkHref);
				  
				  log.write("-Processing link " + linkHref);
				  
				  if( nodeHref == null && !linkText.isEmpty())
				  {
					  log.write(" not in graph; adding it\n");
					  // Node not in Graph, add it and create edge linkHref->head
					  // If linkHref is not in graph, edge linkHref->head cannot create cycle
					  nodeHref = Graph.addNodeEdge(head, linkHref, linkText);
					  					  					  
					  // Increase counters
					  addedNodes++;
					  addedEdges++;
				  }else if (nodeHref != null)
				  {
					  if(head.DPFrom.contains(nodeHref.id)) // head->linkHref creates directed cycle
					  {		
						  log.write(" creates directed cycle\n");
						  
						  // Increase BL count of nodeHref
						  Graph.increaseBackLinksOf(nodeHref); 
						  
						  // Increase vBL of nodes in nodeHref.DPFrom
						  for(Integer n : head.DPFrom)
								  Graph.increaseVBackLinksOf(n); 
					  }
					  else if(Graph.existsEndLabel(nodeHref, linkText)) // there is a edge ending at end nodeHref with same label
					  {
						  log.write(" same endLabel exists\n");
						  
						  // Increase BL count of nodeHref
						  Graph.increaseBackLinksOf(nodeHref); 
						  
						  // Increase vBL of nodes in nodeHref.DPFrom
						  for(Integer n : head.DPFrom)
								  Graph.increaseVBackLinksOf(n); 
					  }
					  else // linkHref->head creates no directed cycle; add edge 
					  {
						  log.write(" is in graph; no directed cycle\n");
						  Graph.addEdge(head, nodeHref, linkText);
						  addedEdges++;
					  }
				  }
			  }
		}
		log.close();

		// Print progress
		System.out.println("Nodes done=" + head.id);
		System.out.println("Nodes overal = " + Graph.getNodesSize());	
		return;
	}
	
	/**
	 * Starting from node head in the graph, it crawls N successive nodes in parallel and calls parseElements when each node is done
	 * Need to check if concurrency problems in the Graph arise 
	 * */
	public static void scan(Integer headId) throws IOException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
	{		
		// Number of parallel requests
		Integer N = 500;
		
		boolean done = false;
		
		// Start scanning
		do{				
			// Parallel request upto tail
			Integer tail = headId + N;
			
			ArrayList<SparCrawl> crawlers = new ArrayList<>();

			if(headId == 1)// start node; only one thread
			{
				// Create thread
				SparCrawl w = new SparCrawl(Graph.getNodeById(headId));
				crawlers.add(w);
	        	new Thread(w).start();
	        	
	        	// Move head
	        	headId++;;
			}else if (tail >= Graph.getNodesSize() && headId!=1) // reached end row; threads upto to that
			{
				while (headId < Graph.getNodesSize()){
					// Create thread 
					SparCrawl w = new SparCrawl(Graph.getNodeById(headId));
					crawlers.add(w);
		        	new Thread(w).start();
		        	
		        	// Move head
		        	headId++;
				}
				done=true;
			}else// none of above; start N threads
			{
				while(headId < tail){
					// Create thread
					SparCrawl w = new SparCrawl(Graph.getNodeById(headId));
					crawlers.add(w);
		        	new Thread(w).start();
		        	
		        	// Move head
		        	headId++;
				}
			}
			
		    // Retrieve results
		    for (SparCrawl c : crawlers) 
		    {
		    	Elements links = c.waitForResults();
		    	if(links != null)
		    	{
		    		Node current = c.getNode();
		    	//	PrintWriter nodeOut = new PrintWriter(new FileOutputStream("./cnn_nodes/node_"+current.id));
		    		//nodeOut.write(current.html);
		    		current.html = null; //to save memory; not needed anymore
		    		//nodeOut.close();
		    		parseElement(current, links);
		    	}
			}
			
			// Save to DB every 3000 new nodes or when done
			if(addedNodes > 2000 || done){
				System.out.println("Adding " + addedNodes + " nodes and " + addedEdges + " edges to db.");
				Graph.transferToDB(db);
				
				//reset counters
				addedEdges=0;
				addedNodes=0;
			}		
		}while(!done);
	}


	public static void main(String[] args) throws IOException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
	{		
		// Current website
		TopLevel = new String("edition.cnn.com");
		
		// Init tables in db
		db = new SparDBHandler("cnn_nodes", "cnn_edges");
		//db.dropTables();
		db.createTables();
		
		// Initialize graph
		Graph = db.loadGraph();
		if(Graph.getNodesSize() != 0)
		{
			Graph.setIndOfLastEdge(Graph.getEdgesSize());
			Graph.setIndOfLastNode(Graph.getNodesSize());
		}
		else
		{				
			Node root = new Node(1,"http://edition.cnn.com/","",0);
			//Node head = new Node(2,"http://www.bbc.com/","",0);
			Graph.addNode(root);
			//Graph.addNode(head);
		}
		
		//scan(1);
		//scan(Graph.firstNotScanned().id);

		PrintWriter pr  = new PrintWriter(new FileOutputStream("endlabel.txt",true));

		SortedMap<String, Integer> map = new TreeMap<String, Integer>();

		for (Edge e : Graph.edgeSet())
		{
			pr.write(e.label +"\t" + Graph.getNodeById(e.start).backLinks + "\t" +Graph.getNodeById(e.end).backLinks+"\n");	
		}
	
		
//		String s = new String();
//		do{
//			Scanner scanner = new Scanner (System.in);
//			System.out.print("Enter node url");  
//			s = scanner.next(); // Get what the user types.
//			System.out.println(s);
//			Node ns = Graph.getNodeByUrl(s);
//			
//			Queue<Node> q = new LinkedList<Node>();
//			q.add(Graph.getNodeByUrl(s));
//			
//			
//			
//		}while(!s.equals("exit"));
	}
	
	
	
	
	
	
}
 