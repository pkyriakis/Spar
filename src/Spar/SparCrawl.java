package Spar;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import Spar.SparGraph.*; 

/**
 * Crawler thread; Given a node object, it scans the url it contains and sets the results to the same object (links, title, html) 
 * 
 * */
public class SparCrawl implements Runnable {

	private Node node;
	private Elements links;
    
    private boolean exception = false;
    
    
    private final Object lock = new Object();

    public SparCrawl(Node node) {
        this.node = node;
    }

    public Node getNode()
    {
    	return this.node;
    }
    
    @Override
    public void run() {
        try {
        	// Download url
            Document doc = Jsoup.connect(this.node.url).get();

            // Get links
            Elements links = doc.select("a"); 
                        
            // Update results
            synchronized (lock) {
                // Set node title
                node.title = doc.title();

                // HTML source; for future use
                node.html = doc.body().toString();
                
                // Set node to scanned
            	this.node.scanned=true;
            	
            	// Set the links contained in node
                this.links = links;
                
                lock.notifyAll();
            }
        } catch (Exception e) {
        	// Exception occurred; set links to null and notify lock
            e.printStackTrace();
            synchronized (lock) {
            	this.node.scanned=false;
            	this.links = null;
                this.exception = true;
                lock.notifyAll();
            }
        }
    }

    public Elements waitForResults() {
        synchronized (lock) {
            try {
                while (this.links == null && !this.exception) {
                    lock.wait();
                }
                return this.links;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return null;
        }
    }
}
