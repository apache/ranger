package com.xasecure.server.tomcat;

import java.io.PrintWriter;
import java.net.Socket;

public class StopEmbededServer extends EmbededServer {

	private static final String SHUTDOWN_HOSTNAME = "localhost" ;
	
	public static void main(String[] args) {
		new StopEmbededServer(args).stop();
	}

	public StopEmbededServer(String[] args) {
		super(args);
	}
	
	public void stop() {
		
		try {
			
			int shutdownPort = getIntConfig("service.shutdownPort", DEFAULT_SHUTDOWN_PORT ) ;
			
			String shutdownCommand = getConfig("service.shutdownCommand", DEFAULT_SHUTDOWN_COMMAND ) ;
			
			Socket sock = new Socket(SHUTDOWN_HOSTNAME,shutdownPort) ;
			
			PrintWriter out = new PrintWriter(sock.getOutputStream(), true) ;
			
			out.println(shutdownCommand) ;
			
			out.flush(); 
			
			out.close();
		}
		catch(Throwable t) {
			System.err.println("Server could not be shutdown due to exception:" +  t) ;
			System.exit(1);
		}
	}
	

}
