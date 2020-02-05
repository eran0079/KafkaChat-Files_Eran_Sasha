package kaf.ka.eran.sasha;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.kafka.clients.consumer.Consumer;

//public static class to do stuff for us
public final class Hashy{
	
	//hash to hold all the consumers with their topics (exclusive for 1 client)
	private static Hashtable<String, Consumer<String, String>> consumersList = new Hashtable<>();

	// path of the configuration file
	private static final String CONFIG_PATH = "configuration.txt";
	
	/**
	 * function that checks if the client is registered to a topic
	 * @param topicName - the topic name
	 * @return - returns true if registered, else false.
	 */
	public static synchronized boolean checkIfRegistered(String topicName){
			
		return consumersList.containsKey(topicName);
	}
	
	/**
	 * try and add to a topic (only if not yet registered)
	 * @param topicName - the topic name
	 * @param consumer - the consumer that tries to join
	 * @return true if we added, false if we already in
	 */
	public static synchronized boolean addToTopic(String topicName, Consumer<String, String> consumer){
		
		if(checkIfRegistered(topicName)){  				
			return false;
		}
		else{
			consumersList.put(topicName, consumer);
			return true;
		}
	}
	
	/**
	 * remove from topic (only if already registered)
	 * @param topicName - the topic name
	 * @return true if we removed, false if we werent registered.
	 */
	public static synchronized boolean removeFromTopic(String topicName){

		if(checkIfRegistered(topicName)){  	
			consumersList.get(topicName).wakeup();
			consumersList.remove(topicName);
			return true;
		}
		else{
			return false;
		}
	}
		
	/**
	 * reading the broker address from the config file
	 * @return return the string inside the config file - telling its IP
	 * @throws IOException
	 */
	public static synchronized String getBroker() throws IOException {
		String brokerAddress;
		BufferedReader brFile = new BufferedReader(new FileReader(CONFIG_PATH));
		brokerAddress = brFile.readLine().trim();
		brFile.close();
		return brokerAddress;
	}
	
	/**
	 * just as the name says, we are done here. lets wrap it up and throw exception to all the consumers.
	 */
	public static synchronized void cleanSlateProtocol(){
		for (Consumer<String, String> consumer : consumersList.values()) {
			consumer.wakeup();
		}
		consumersList.clear();
	}
}
