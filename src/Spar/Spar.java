package Spar;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
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
	private static LinkedList<String> LinksPending = new LinkedList<String>();
	
	// Urls done
	private static LinkedList<String> LinksScanned = new LinkedList<String>();
	
	// Counts the urls added to the rows; used in order to save them to db every eg 1000 urls
	private static Integer addedCount = 0;
	
	static //Make sure each link contains this
	String TopLevel = "";
	
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
		Node nodeUrl = Graph.getNodeOf(url);
				
		for (Element link : links) {
			  // Get href of current link
			  String linkHref = link.attr("abs:href");
			  // Get text on link
			  String linkText = link.text();

			  /**
			   * Add current Href to the Links the pending list iff
			   * 	it points to the same website as parentlink - scanning only one website for now
			   * 	it is not in pending list
			   * 	it has not been scanned
			   * 	the link text is not empty
			   * */
			  if(linkHref.contains(TopLevel) && !LinksScanned.contains(linkHref) 
					  && !LinksPending.contains(linkHref) && !linkText.isEmpty() && !linkHref.contains("#")){
				  LinksPending.add(linkHref);
				  Log(outPendingLinks, linkHref+"\n");
			  }
			  
			  
			  // Only add links from the same host to db
			  if(linkHref.contains(TopLevel) & !linkHref.contains("#"))
			  {

				  Log(outLog, "-Processing link " + linkHref + " ...");
				  
				  
				  if(Graph.getNodeOf(linkHref) != null && !linkText.isEmpty())// not in db; add it
				  {
					  // Create node for linkHref; title empty for now
					  Integer nodeHrefId = Graph.getNodesSize()+1;
					  Node nodeHref = new Node(nodeHrefId,linkHref,"",0);
					  
					  // Create edge url->linkHref
					  Integer edgeUrlHrefId = Graph.getEdgesSize() + 1;
					  Edge edgeUrlHref = new Edge(edgeUrlHrefId, nodeUrl.id, nodeHrefId, linkText);
					  // LOG 
					  Log(outLog, "not it db; adding it\n");
					  
					  // Add
					  addedCount++;
				  }else if(!linkText.isEmpty())
				  {
					  	// check if edge urlRow->HrefRow creates directed cycle
					  	// if yes update the backlinks of HrefRow
					  	// else add HrefRow

				  }
			  }
			}
		
		// Log
		Log(outLog, "\n");
		
		// Print progress
		System.out.println("Links done="+LinksScanned.size());
		System.out.println("Links left="+LinksPending.size()+"\n");

		// Call garbage collector
		System.gc();
		
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
		
	public static void main(String[] args) throws IOException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
	{		

		// Load lists from previous run
		//LoadFromFile();
			

		
//		//Logging
//		outLog = new PrintWriter(new FileOutputStream("log.txt",true));
//		outPendingLinks = new PrintWriter(new FileOutputStream("pending.txt",true));
//		
//		// Used for tracking the first row not addded in db
//		Integer index = Rows.size();
//		
//		// Start scanning
//		for(int i = 0; i < LinksPending.size(); i++){			
//			scanWebsite(LinksPending.elementAt(i));
//			if(addedCount > 1000){// 1000 new rows added to Rows; save them to db
//				System.out.println("Adding " + addedCount + " links to db.");
//				while (addedCount > 0)
//				{
//					db.putInTable(Rows.elementAt(index));
//					index++;
//					addedCount--;
//				}
//			}
//		}
		
		
		// Close file and connection
		outLog.close();
		outPendingLinks.close();
	}
}
 