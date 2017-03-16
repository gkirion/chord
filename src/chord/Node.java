package chord;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Node {
	
	private HashMap<String, String> hashMap;
	private MessageDigest md;
	private JsonObject response;
	private int nodeId, previousNodeId, nextNodeId;
	private int nodePort, previousPort, nextPort;
	private static final int startPort = 9000;
	
	public Node() {
		hashMap = new HashMap<>();
		try {
			md = MessageDigest.getInstance("SHA1");
			nodeId = 0;
			previousNodeId = 0;
			nextNodeId = 0;
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Node(String key) {
		hashMap = new HashMap<>();
		try {
			md = MessageDigest.getInstance("SHA1");
			nodeId = hash(key);
			previousNodeId = nodeId;
			nextNodeId = nodeId;
			nodePort = startPort +  Integer.parseInt(key);
			previousPort = nodePort;
			nextPort = nodePort;
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Runnable server = new Server(nodePort);
		Thread t1 = new Thread(server);
		t1.start();
	}
	
	public void join() {
		try {
			Socket socket = new Socket("", startPort + 1); // connect to node 1
			JsonObject jsonObject = new JsonObject();
			jsonObject.addProperty("type", "join_request");
			jsonObject.addProperty("from", nodePort);
			PrintWriter out = new PrintWriter(socket.getOutputStream());
			out.print(jsonObject);
			out.flush();
			socket.close();
			System.out.println("waiting response");
			joinResponse();
			System.out.println("got response");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private synchronized void joinResponse() {
		// wait for join response
		while (response == null || !response.get("type").getAsString().equals("set_next_response")) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		response = null;
		notifyAll();
	}
	
	public void leave() {
		try {
			Socket socket;
			JsonObject jsonObject;
			PrintWriter out;
			jsonObject = new JsonObject();
			jsonObject.addProperty("type", "leave_request");
			jsonObject.addProperty("from", nodePort);
			jsonObject.addProperty("next", nextPort);
			jsonObject.addProperty("previous", previousPort);
			if (previousNodeId != nextNodeId) {
				socket = new Socket("", previousPort); // connect to previous node
				out = new PrintWriter(socket.getOutputStream());
				out.print(jsonObject);
				out.flush();
				socket.close();
				System.out.println("waiting response from previous node");
				leaveResponse();
				System.out.println("got response from previous node");
			}
			int id;
			JsonObject js = new JsonObject();
			for (Entry<String, String> entry : hashMap.entrySet()) {
				id = hash(entry.getKey());
				if (isBetween(id, previousNodeId, nodeId)) {
					js.addProperty(entry.getKey(), entry.getValue());
				}
			}
			jsonObject.add("value", js);
			socket = new Socket("", nextPort); // connect to next node 
			out = new PrintWriter(socket.getOutputStream());
			out.print(jsonObject);
			out.flush();
			socket.close();
			System.out.println("waiting response from next node");
			leaveResponse();
			System.out.println("got response from next node, leaving");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private synchronized void leaveResponse() {
		// wait for leave response
		while (response == null || !response.get("type").getAsString().equals("leave_response")) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		response = null;
		notifyAll();
	}
	
	public int getNodeId() {
		return nodeId;
	}
	
	public String get(String key) {
		int id = hash(key);
		if (isLocal(id)) {
			System.out.printf("node %d: get request served locally\n", nodeId);
			return hashMap.get(key);
		}	
		else { // key is not local
			getRequest(key);
			return getResponse(key);
		}	
	}
	
	public void put(String key, String value) {
		int id = hash(key);
		if (isLocal(id)) {
			System.out.printf("node %d: put request served locally\n", nodeId);
			hashMap.put(key, value);
		}
		else { // key is not local
			putRequest(key, value);
			putResponse(key);
		}
	}
	
	public void delete(String key) {
		put(key, null);
	}
	
	private void putRequest(String key, String value) {
		// prepare request
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("type", "put_request");
		jsonObject.addProperty("from", nodePort);
		jsonObject.addProperty("key", key);
		jsonObject.addProperty("value", value);
		try {
			// send it to the next node in the ring
			Socket socket = new Socket("", nextPort);
			PrintWriter out = new PrintWriter(socket.getOutputStream());
			out.print(jsonObject);
			out.flush();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private synchronized void putResponse(String key) {
		// while there is no response, leave the lock and go to sleep
		while (response == null || !response.get("type").getAsString().endsWith("put_response") || !response.get("key").getAsString().equals(key)) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		int from = response.get("from").getAsInt();
		int id = hash(String.valueOf(from - startPort));
		System.out.printf("node %d: put request served remotely from node %d\n", nodeId, id);
		response = null;
		notifyAll();
	}
	
	private void getRequest(String key) {
		// prepare request
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("type", "get_request");
		jsonObject.addProperty("from", nodePort);
		jsonObject.addProperty("key", key);
		try {
			// send it to the next node in the ring
			Socket socket = new Socket("", nextPort);
			PrintWriter out = new PrintWriter(socket.getOutputStream());
			out.print(jsonObject);
			out.flush();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private synchronized String getResponse(String key) {
		// while there is no response, leave the lock and go to sleep
		String value;
		while (response == null || !response.get("type").getAsString().endsWith("get_response") || !response.get("key").getAsString().equals(key)) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		int from = response.get("from").getAsInt();
		int id = hash(String.valueOf(from - startPort));
		System.out.printf("node %d: get request served remotely from node %d\n", nodeId, id);
		value = response.get("value").getAsString();
		response = null;
		notifyAll();
		return value;
	}
	
	private synchronized void produceResponse(JsonObject jsonObject) {
		// while response hasn't been read, leave the lock and go to sleep
		while (response != null) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// place response
		response = jsonObject.getAsJsonObject();
		notifyAll();
	}
	
    public int hash(String key) {
    	byte[] result = md.digest(key.getBytes()); // get hash
        // modulo 2^16 fast way
        return result[result.length - 1] & 0xff | (result[result.length - 2] & 0xff) << 8;
    }
    
    public boolean isLocal(int id) {
    	if (id > previousNodeId && id <= nodeId) {
	    	return true;
	    }
	    else if (nodeId <= previousNodeId) {
	    	if (id > previousNodeId || id <= nodeId) {
	    		return true;
	    	}
	    }
    	return false;
    }
    
    public boolean isBetween(int id, int previousId, int nextId) {
    	if (id > previousId && id <= nextId) {
	    	return true;
	    }
	    else if (nextId <= previousId) {
	    	if (id > previousId || id <= nextId) {
	    		return true;
	    	}
	    }
    	return false;
    }
    
    private class Server implements Runnable {
    	
    	private int port;
    	
    	public Server(int port) {
    		this.port = port;
    	}

		@Override
		public void run() {
			System.out.printf("Server for node %d started on port %d\n", nodeId, port);
			try {
				ServerSocket serverSocket = new ServerSocket(port);
				while (true) {
					Socket clientSocket = serverSocket.accept();
					Client client = new Client(clientSocket);
					Thread thread = new Thread(client);
					thread.start();
				}	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    	
    }
    
    private class Client implements Runnable {
    	
    	private Socket socket;
    	
    	public Client(Socket socket) {
    		this.socket = socket;
    	}

		@Override
		public void run() {
			try {
				Scanner in = new Scanner(socket.getInputStream());
				StringBuilder sb = new StringBuilder();
				// read message
				while (in.hasNextLine()) {
					sb.append(in.nextLine());
				}
				socket.close();
				JsonObject jsonObject = new JsonParser().parse(sb.toString()).getAsJsonObject();
				//System.out.println(jsonObject);
				String type = jsonObject.get("type").getAsString();
				int from = jsonObject.get("from").getAsInt();
				if (type.equals("get_request")) {
					System.out.println("got get_request");
					String key = jsonObject.get("key").getAsString();
					int id = hash(key);
					if (isLocal(id)) { // send response to who initiated the request
						String value = hashMap.get(key);
						jsonObject.addProperty("type", "get_response");
						jsonObject.addProperty("from", nodePort);
						if (value != null) {
							jsonObject.addProperty("value", value);
						}
						else {
							jsonObject.addProperty("value", "null");
						}
						//System.out.println(jsonObject);
						socket = new Socket("", from);
						PrintWriter out = new PrintWriter(socket.getOutputStream());
						out.print(jsonObject);
						out.flush();
						socket.close();
					}
					else { // forward request to the next node in the circle
						System.out.println(jsonObject);
						socket = new Socket("", nextPort);
						PrintWriter out = new PrintWriter(socket.getOutputStream());
						out.print(jsonObject);
						out.flush();
						socket.close();
					}
				}
				else if (type.equals("get_response")) {
					produceResponse(jsonObject);
				}
				else if (type.equals("put_request")) {
					String key = jsonObject.get("key").getAsString();
					String value = jsonObject.get("value").getAsString();
					int id = hash(key);
					if (isLocal(id)) { // if node is responsible for the key store it locally
						hashMap.put(key, value);
						jsonObject.addProperty("type", "put_response");
						jsonObject.addProperty("from", nodePort);
						jsonObject.addProperty("value", "ok");
						System.out.println(jsonObject);
						socket = new Socket("", from);
						PrintWriter out = new PrintWriter(socket.getOutputStream());
						out.print(jsonObject);
						out.flush();
						socket.close();
					}
					else { // forward request to the next node in the circle
						System.out.println(jsonObject);
						socket = new Socket("", nextPort);
						PrintWriter out = new PrintWriter(socket.getOutputStream());
						out.print(jsonObject);
						out.flush();
						socket.close();
					}
				}
				else if (type.equals("put_response")) {
					produceResponse(jsonObject);
				}
				else if (type.equals("join_request")) {
					String key = String.valueOf((from - startPort));
					int id = hash(key);
					if (isLocal(id)) {
						jsonObject.addProperty("type", "join_response");
						jsonObject.addProperty("from", nodePort);
						jsonObject.addProperty("previous", previousPort);
						System.out.println(jsonObject);
						socket = new Socket("", from);
						PrintWriter out = new PrintWriter(socket.getOutputStream());
						out.print(jsonObject);
						out.flush();
						socket.close();
					}
					else { // forward request to the next node in the circle
						System.out.println(jsonObject);
						socket = new Socket("", nextPort);
						PrintWriter out = new PrintWriter(socket.getOutputStream());
						out.print(jsonObject);
						out.flush();
						socket.close();
					}
				}
				else if (type.equals("join_response")) {
					String key = String.valueOf((from - startPort));
					nextNodeId = hash(key);
					nextPort = from;
					key = String.valueOf(jsonObject.get("previous").getAsInt() - startPort);
					previousNodeId = hash(key);
					previousPort = jsonObject.get("previous").getAsInt();
					jsonObject.remove("previous");
					jsonObject.addProperty("type", "keys_request");
					jsonObject.addProperty("from", nodePort);
					jsonObject.addProperty("to", previousPort);
					socket = new Socket("", nextPort);
					PrintWriter out = new PrintWriter(socket.getOutputStream());
					out.print(jsonObject);
					out.flush();
					socket.close();
				}
				else if (type.equals("leave_request")) {
					if (from == previousPort) {
						JsonObject resp = jsonObject.get("value").getAsJsonObject();
						for (Entry<String, JsonElement> entry : resp.entrySet()) {
							System.out.println(entry.getKey() + ": " + entry.getValue());
							hashMap.put(entry.getKey(), entry.getValue().getAsString());
						}
						int previous = jsonObject.get("previous").getAsInt();
						previousPort = previous;
						previousNodeId = hash(String.valueOf(previous - startPort));
						jsonObject.remove("value");
					}
					if (from == nextPort) {
						int next = jsonObject.get("next").getAsInt();
						nextPort = next;
						nextNodeId = hash(String.valueOf(next - startPort));
					}
					jsonObject.remove("next");
					jsonObject.remove("previous");
					jsonObject.addProperty("type", "leave_response");
					jsonObject.addProperty("from", nodePort);
					socket = new Socket("", from);
					PrintWriter out = new PrintWriter(socket.getOutputStream());
					out.print(jsonObject);
					out.flush();
					socket.close();
				}
				else if (type.equals("leave_response")) {
					produceResponse(jsonObject);
				}
				else if (type.equals("keys_request")) {
					int id, node_id = hash(String.valueOf(from - startPort));
					int to = hash(String.valueOf(jsonObject.get("to").getAsInt() - startPort));
					sb = new StringBuilder();
					JsonObject js = new JsonObject();
					for (Entry<String, String> entry : hashMap.entrySet()) {
						id = hash(entry.getKey());
						if (isBetween(id, to, node_id)) {
							js.addProperty(entry.getKey(), entry.getValue());
						}	
					}
					jsonObject.remove("to");
					jsonObject.addProperty("type", "keys_response");
					jsonObject.addProperty("from", nodePort);
					jsonObject.add("value", js);
					System.out.println(jsonObject);
					socket = new Socket("", from);
					PrintWriter out = new PrintWriter(socket.getOutputStream());
					out.print(jsonObject);
					out.flush();
					socket.close();
				}
				else if (type.equals("keys_response")) {
					JsonObject resp = jsonObject.get("value").getAsJsonObject();
					for (Entry<String, JsonElement> entry : resp.entrySet()) {
						System.out.println(entry.getKey() + ": " + entry.getValue());
						hashMap.put(entry.getKey(), entry.getValue().getAsString());
					}
					jsonObject.remove("value");
					jsonObject.addProperty("type", "set_previous");
					jsonObject.addProperty("from", nodePort);
					socket = new Socket("", nextPort);
					PrintWriter out = new PrintWriter(socket.getOutputStream());
					out = new PrintWriter(socket.getOutputStream());
					out.print(jsonObject);
					out.flush();
					socket.close();
				}
				else if (type.equals("set_next")) {
					nextPort = from;
					nextNodeId = hash(String.valueOf(from - startPort));
					jsonObject.addProperty("type", "set_next_response");
					jsonObject.addProperty("from", nodePort);
					socket = new Socket("", nextPort);
					PrintWriter out = new PrintWriter(socket.getOutputStream());
					out = new PrintWriter(socket.getOutputStream());
					out.print(jsonObject);
					out.flush();
					socket.close();
				}
				else if (type.equals("set_next_response")) {
					produceResponse(jsonObject);
				}
				else if (type.equals("set_previous")) {
					previousPort = from;
					previousNodeId = hash(String.valueOf(from - startPort));
					jsonObject.addProperty("type", "set_previous_response");
					jsonObject.addProperty("from", nodePort);
					socket = new Socket("", previousPort);
					PrintWriter out = new PrintWriter(socket.getOutputStream());
					out = new PrintWriter(socket.getOutputStream());
					out.print(jsonObject);
					out.flush();
					socket.close();
				}
				else if (type.equals("set_previous_response")) {
					jsonObject.addProperty("type", "set_next");
					jsonObject.addProperty("from", nodePort);
					socket = new Socket("", previousPort);
					PrintWriter out = new PrintWriter(socket.getOutputStream());
					out.print(jsonObject);
					out.flush();
					socket.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
    	
    }

}
