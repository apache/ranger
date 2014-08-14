package com.xasecure.unixusersync.poc;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
 
public class RestClientPost {
 
  public static void main(String[] args) {
 
	try {
 
		Client client = Client.create();
 
		WebResource webResource = client
				   .resource("http://domain/service/xusers/group/testGroup/user/testUser");
		ClientResponse response = webResource.type("application/json")
				.post(ClientResponse.class);
		 
		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
			     + response.getStatus());
		}
 
		System.out.println("Output from Server .... \n");
		String output = response.getEntity(String.class);
		System.out.println(output);
 
	  } catch (Exception e) {
 
		e.printStackTrace();
 
	  }
 
	}
}