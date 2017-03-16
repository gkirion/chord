package chord;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Scanner;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Chord {
	
	public static void main(String[] args) {
		Node[] node = new Node[10];
		for (int i = 0; i < 10; i++) {
			node[i] = new Node(String.valueOf(i + 1));
			node[i].join();
		}
		Random random = new Random();
		int n;
		FileReader reader;
		HashMap<String, String> hashMap = new HashMap<>();
		try {
			reader = new FileReader("/home/george/Desktop/insert.txt");
			Scanner in = new Scanner(reader);
			while (in.hasNextLine()) {
				String line = in.nextLine();
				String[] pair = line.split(",");
				System.out.printf("key: %s, value: %s\n", pair[0], pair[1]);
				n = random.nextInt(10);
				System.out.printf("sending put request to node %d\n", n);
				hashMap.put(pair[0], pair[1]);
				node[n].put(pair[0], pair[1]);
			}
			reader = new FileReader("/home/george/Desktop/query.txt");
			in = new Scanner(reader);
			while (in.hasNextLine()) {
				String line = in.nextLine();
				System.out.printf("value: %s\n", line);
				n = random.nextInt(10);
				System.out.printf("sending put request to node %d\n", n);
				System.out.println(node[n].get(line));
				if (!node[n].get(line).equals(hashMap.get(line))) {
					System.err.println("different");
				}
			}
			System.out.println(node[3].get("tzopaty"));

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*for (int i = 0; i < 1000; i++) {
			n = random.nextInt(10);
			System.out.printf("sending put request to node %d\n", n);
			node[n].put(keys[i], String.valueOf(random.nextInt()));
		}
		for (int i = 0; i < 1000; i++) {
			n = random.nextInt(10);
			System.out.printf("sending get request to node %d\n", n);
			System.out.println(node[n].get(keys[i]));
		}*/
		System.out.println("Hello from chord distributed hash table!");
	}
	
}
