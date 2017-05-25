package Spar;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;
import org.jsoup.*;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import Spar.SparGraph.*;
import Spar.SparDBHandler.*;

import java.sql.*;

public class Spar {

	// The db handler
	static SparDBHandler db;
	
	// The db data
	static SparGraph Graph;
		
	// For logging to file
	private static PrintWriter outLog;
	
	// Log links pending
	private static PrintWriter outPendingLinks;
	
	// Vector of Pending urls
	private static Vector<String> LinksPending = new Vector<String>();
	
	// Urls done
	private static HashSet<String> LinksScanned = new HashSet<String>();
	
	// Counts the nodes and edges added to graph
	private static Integer addedNodes = 0;
	private static Integer addedEdges = 0;
	
	static //Make sure each link contains this
	String TopLevel = "https://www.ethz.ch/en";
	
	// Keeps track of the url that have been backlinked
	private static Vector<String> backLinked = new Vector<String>();
	
	/**
	 * Log to file; should already exist
	 * */
	
	private static void Log(PrintWriter out, String text) 
	{
		out.write(text);
		return;
	}
	
	
	/**
	 * Convert string to url; no www, needed for jsoup
	 */
	private static URL stringToURL(String url) throws MalformedURLException
	{
		// Remove www, add protocol and return a URL
		url = url.replace("www.", "");
		
		if(!url.contains("http"))
			url="http://"+url;

		return new URL(url);
	}
	
	
	private static void scanWebsite(String url) throws MalformedURLException
	{	
		// If already scanned, return
		if(LinksScanned.contains(url))
			return;
						
		// Add it to done list
		LinksScanned.add(url);
		
		// Grap page
		Document page=null;
		try {
			page = Jsoup.connect(url).get();
		} catch (IOException e1) {
		}
		if(page==null)return;
		
		// LOG 
		Log(outLog, "Scanning "+url+"\n");
				
		// Get links in page
		Elements links = page.getElementsByTag("a");
		
		// Get node of url
		Node nodeUrl = Graph.getNodeByUrl(url);
				
		// Set nodeUrl to scanned
		nodeUrl.setScanned();
		
		// Set the DP ending at nodeUrl
		HashSet<Node> dpStarts = new HashSet<Node>(); 
				
		for (Element link : links) {
			  // Get href of current link
			  String linkHref = link.attr("abs:href");
			  // Get text on link
			  String linkText = link.text();

//			  /**
//			   * Add current Href to the Links the pending list iff
//			   * 	it points to the same website as parentlink - scanning only one website for now
//			   * 	it is not in pending list
//			   * 	it has not been scanned
//			   * 	the link text is not empty
//			   * */
//			  if(linkHref.contains(TopLevel) && !LinksScanned.contains(linkHref) 
//					  && !LinksPending.contains(linkHref) && !linkText.isEmpty() && !linkHref.contains("#")){
//				  LinksPending.add(linkHref);
//				  Log(outPendingLinks, linkHref+"\n");
//			  }
			  
			  
			  // Only add links from the same host to db
			  if(linkHref.contains(TopLevel) & !linkHref.contains("#") && !linkText.isEmpty())
			  {

				  Log(outLog, "-Processing link " + linkHref + " ...");
				  
				  // Get node of Href; if any
				  Node nodeHref = Graph.getNodeByUrl(linkHref);
				  
				  if( nodeHref == null)
				  {
					  // Node in Graph, add it and create edge linkHref->url
					  // If linkHref is not in graph, edge linkHref->url cannot create cycle
					  Graph.addNodeEdge(nodeUrl, linkHref, linkText);
					  
					  // LOG 
					  Log(outLog, "not in graph; adding it\n");
					  
					  // Increase counters
					  addedNodes++;
					  addedEdges++;
				  }else
				  {
					  if(dpStarts.contains(nodeHref)) // url->linkHref creates directed cycle
					  {		
						  Log(outLog, "creates directed cycle\n");
						  Graph.increaseBackLinksOf(nodeHref.id); // increase its backlink count
					  }else // linkHref->url creates no directed cycle; add edge 
					  {
						  Log(outLog, "is graph; no directed cycle\n");
						  Graph.addEdge(nodeUrl, nodeHref, linkText);
						  addedEdges++;
					  }
				  }
			  }
			}
		
		// Log
		Log(outLog, "\n");
		
		// Print progress
		System.out.println("Links done="+LinksScanned.size());
		System.out.println("Links done="+LinksPending.size());	
		return;
	}
	
	private static void LoadFromFile() throws IOException
	{
		BufferedReader br = new BufferedReader(new FileReader("pending.txt"));
		for(String line; (line = br.readLine()) != null; ) 
		{
			line.replace("\n", "");
		    LinksPending.add(line);
		}
		    
		br = new BufferedReader(new FileReader("log.txt"));
		for(String line; (line = br.readLine()) != null; ) 
		{
			if(line.contains("Scanning ")){
				line.replace("\n", "").replace("Scanning ", "");
		    	LinksScanned.add(line);
		    }
		}
	}
	
	
	// Temporary helper function
	public static void scan() throws IOException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
	{
		// Set initial link and and node
		LinksPending.add("https://www.ethz.ch/en.html");

		// Initialize graph
		Graph = new SparGraph();
		
		// Set initial link and and node
		LinksPending.add("https://www.ethz.ch/en.html");
		Graph.addNode(new Node(1,"https://www.ethz.ch/en.html","",0));

		// Init tables in db
		db = new SparDBHandler("eth_nodes", "eth_edges");
		db.dropTables();
		db.createTables();
				
		//Logging
		outLog = new PrintWriter(new FileOutputStream("log.txt",true));
		outPendingLinks = new PrintWriter(new FileOutputStream("pending.txt",true));
						
		// Start scanning
		for(int i = 0; i < LinksPending.size(); i++){		
			db = new SparDBHandler("eth_nodes", "eth_edges");
			scanWebsite(LinksPending.elementAt(i));
			if(addedNodes > 500){// save them to db
				System.out.println("Adding " + addedNodes + " nodes and " + addedEdges + " edges to db.");
				Graph.transferToDB(db);
				db.close();
				//reset counters
				addedEdges=0;
				addedNodes=0;
			}
		}
		
		
		// Close file and connection
		outLog.close();
		outPendingLinks.close();
		
	}
		
	public static void main(String[] args) throws IOException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
	{		
		//scan();		
		
		db = new SparDBHandler("eth_nodes", "eth_edges");
		SparGraph Graph = db.loadGraph();
		System.out.println(Graph.getNodeById(1).url);
		Node last=Graph.lastScanned();
		
		
		
		
		
		
		
		
		
		
	}
	
	
	
	
	
	
}
 