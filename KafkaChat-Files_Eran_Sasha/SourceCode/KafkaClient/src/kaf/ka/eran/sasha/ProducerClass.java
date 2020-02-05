package kaf.ka.eran.sasha;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ProducerClass {
	
	//broker IP + login name of the user
    private static String BOOTSTRAP_SERVERS;
    private static String LOGIN_NAME;
    private static String MESSAGE = "message";
    private static String FILES_FOLDER = "filesToSend\\";
    private static BufferedReader brIn = null;

	public static void main(String[] args) {
		//read file configuration to get the broker IP
		try {
			BOOTSTRAP_SERVERS = Hashy.getBroker();
		} catch (IOException ioe) {
			System.out.println("Error reading file: " + ioe.getMessage()
					+ "\nThe File should be at: " + System.getProperty("user.dir"));
			return;
		}
		catch (NumberFormatException nfe){
			System.out.println("File format corrupted!");
			return;
		}

		//Initializing the buffered reader from keyboard.
		brIn = new BufferedReader(new InputStreamReader(System.in));		
		
		System.out.println("#########################################\n#\tWelcome to Kafka chat!\t\t#\n#########################################\n"
				+ "Please Enter your name to login(spaces will be ignored):");
		//validate login name.
		do{
			try {
				//read the login name from the user
				LOGIN_NAME = brIn.readLine().trim().replaceAll("\\s","");
				if(LOGIN_NAME.length() == 0){
					System.out.println("Wrong input. Please enter a valid name:");
				}
			} catch (IOException ioe) {
				System.out.println("Failed to read input: "+ ioe.getMessage());
			}
		}while(LOGIN_NAME.length() == 0);
		
		//welcome - entered the right name
		System.out.println("Welcome " + LOGIN_NAME);
		
		//function that print rules
		printRules();
		
		//the entire user interface: 
		while(true){	
			try {
					//read keyboard input
					String userAnswer;
					userAnswer = brIn.readLine().trim();
					String[] sentence = userAnswer.split(" ");
					
					if(sentence.length == 0) {
						continue;
					}
					//switch for user inputs
					switch(sentence[0].toUpperCase()){
						case "JOIN": {
							try {
								//join must be "join topic" else illegal
								if(sentence.length == 2) {
									String topic = sentence[1].toLowerCase();
									//check if topic already registered
									if(Hashy.checkIfRegistered(topic)) {
										System.out.println("Already registered to topic " + topic);
									}
									else{
										//if not registered, create(if not yet created) it and create consumer
										createTopic(topic);
										runConsumer(topic);
										System.out.println("Joined topic: " + topic);
									}
								}
								else {
									System.out.println("Illegal command!");
									printRules();
								}
							} 
					    	catch (ExecutionException ee) {
					    		System.out.println("Error creating topic(Broker un-available): " + ee.getMessage());
								return;
							}
					    	catch (InterruptedException ie) {
								System.out.println("Error creating topic: " + ie.getMessage());
								return;
							} 
					    	catch (TimeoutException te) {
					    		System.out.println("Error creating topic: " + te.getMessage());
					    		return;
							}
					    	catch (Exception e) {
					    		System.out.println("Error creating topic: " + e.getMessage());
					    		return;
							}
							break;
						}
						case "LEAVE":{
							try {
								//leave must be "leave topic" else illegal
								if(sentence.length == 2) {
									String topic = sentence[1].toLowerCase();
									//check if registered to topic
									if(Hashy.checkIfRegistered(topic)) {
										//try to unregister a topic if can
										if(Hashy.removeFromTopic(topic)) {
											System.out.println("Left topic: " + topic);
										}
										else {
											System.out.println("Failed to leave topic.");
										}
									}//not registered, nothing to unregister from.
									else{
										System.out.println("Not yet registered to topic " + topic);
									}
								}
								else {
									System.out.println("Illegal command!");
									printRules();
								}
							}catch (Exception e) {
								System.out.println("Error leaving topic : " + e.getMessage());
							}
							break;
						}
						case "QUIT":{
							//the name says it all.
							Hashy.cleanSlateProtocol();
							System.out.println("Bye Bye!");
							return;
						}
						case "SENDFILE":{
							try {
								//sendfile must be "sendfile topic" else illegal
								if(sentence.length == 2) {
									String topic = sentence[1].toLowerCase();
									String fileName = chooseFile(FILES_FOLDER);
									if(fileName.equals("")) {
										printRules();
										continue;
									}
									createTopic(topic);
									runFileProducer(topic, fileName, BOOTSTRAP_SERVERS);
								}
								else {
									System.out.println("Illegal command!");
									printRules();
								}
							}
					    	catch (ExecutionException ee) {
					    		System.out.println("Error creating topic(Broker un-available): " + ee.getMessage());
								return;
							}
					    	catch (InterruptedException ie) {
								System.out.println("Error creating topic: " + ie.getMessage());
								return;
							} 
					    	catch (TimeoutException te) {
					    		System.out.println("Error creating topic: " + te.getMessage());
					    		return;
							}
					    	catch (Exception e) {
					    		System.out.println("Error creating topic: " + e.getMessage());
					    		return;
							}
							break;
						}
						case "GETFILE":{							
							try {
								//getfile must be "getfile topic" else illegal
								if(sentence.length == 2) {
									String topic = sentence[1].toLowerCase();
									createTopic(topic);
									runFileConsumer(topic, BOOTSTRAP_SERVERS);
								}
								else {
									System.out.println("Illegal command!");
									printRules();
								}
							}catch (Exception e) {
								System.out.println("Error leaving topic : " + e.getMessage());
							}
							break;
						}
						default:{
							//the default is "send" messages  (commands cannot be topics)
							try {
								String message;
								//unknown command  - print rules
								if(sentence.length == 1) {
									System.out.println("Illegal command!");
									printRules();
								}
								else {
									//connect all the parts of the message into 1 string.
									message = String.join(" ", Arrays.copyOfRange(sentence,1,sentence.length));
									String topic = sentence[0].toLowerCase();
									createTopic(topic);
									runProducer(topic,prepareMessage(topic, message));
								}
							}
					    	catch (ExecutionException ee) {
					    		System.out.println("Error creating topic(Broker un-available): " + ee.getMessage());
								return;
							}
					    	catch (InterruptedException ie) {
								System.out.println("Error creating topic: " + ie.getMessage());
								return;
							} 
					    	catch (TimeoutException te) {
					    		System.out.println("Error creating topic: " + te.getMessage());
					    		return;
							}
					    	catch (Exception e) {
					    		System.out.println("Error creating topic: " + e.getMessage());
					    		return;
							}
						}
					}		
			} catch (IOException ioe) {
				System.out.println("Failed to read input. "+ ioe.getMessage());
			}
		}
	}
   
	/**
	 * producer creation function, setting properties and returning the producer.
	 * @return the producers instance
	 */
    private static Producer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); //broker ip
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer_" + LocalDateTime.now()); //must be unique
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //key
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //value
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000); //max delay of sending before exception error (3 sec).
        return new KafkaProducer<>(properties);
    }
        
    /**
     * create the consumer
     * @param topic - the string of the topic name
     * @return the consumer instance
     */
    private static Consumer<String, String> createConsumer(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); //broker ip
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer_" + LocalDateTime.now()); // must be unique
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //key
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //value
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //not to read all the messages from the start.
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }
    
    /**
     * running the consumer, creating new thread of consumer, creating instance of a topic and running
     * @param topic - the string of a topic
     */
	public static void runConsumer(String topic){
        Consumer<String, String> consumer = createConsumer(topic);
        //try to add to a topic, if managed then create it and run.
        if(Hashy.addToTopic(topic, consumer)) {
        	ConsumerClass consumerClass = new ConsumerClass(consumer);
            consumerClass.start();
        }
        else {
        	System.out.println("Failed to register consumer.");
        	consumer.close();
        }
    }

	/**
	 * running the producer, no need new thread as it is the main thread.
	 * @param topic - the topic which we are sending to
	 * @param message - the message to that topic
	 */
    private static void runProducer(String topic, String message) {
        Producer<String, String> producer = createProducer();
        String key = MESSAGE;	//String int that should be unique sort of
        //make the record to be sent from the producer to the broker
    	ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);        
        
		try { //try and send it to the broker.
			producer.send(record).get();
			System.out.println("Message Sent");   
		} 
		catch (InterruptedException e) {
			System.out.println("Error run producer - IntEx: " + e.getMessage());
			
		} 
		catch (ExecutionException e) {
			System.out.println("Failed to send message: " + message);
			
		}
		catch (Exception e) {
			System.out.println("Error run producer - Exception: " + e.getMessage());
			e.printStackTrace();
		}
		finally { //close the producer after you sent it
			producer.flush();
            producer.close();
		}
    }

    /**
     * run the file producer thread to send the file to the kafka
     * @param topic - the topic
     * @param filePath - the file path
     * @param brokerIP - the ip and port of the broker
     */
    private static void runFileProducer(String topic, String filePath, String brokerIP){
    	ProducerFileClass fileProducer = new ProducerFileClass(topic, filePath, brokerIP);
    	fileProducer.start();
    }
    
    /**
     * run the file consumer thread to receive files in a topic
     * @param topic - the topic u download all files from
     * @param brokerIP - the brokers IP we are connected to
     */
    private static void runFileConsumer(String topic, String brokerIP){
    	ConsumerFileClass fileConsumer = new ConsumerFileClass(topic, brokerIP);
    	fileConsumer.start();
    }
    
    /**
     * creating a new topic with the right requirements.
     * @param topic - the string of the topic
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    private static void createTopic(String topic) throws InterruptedException, ExecutionException {
    	
    	//create Admin
    	Properties config = new Properties();
    	config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    	config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000); //time the admin to quit after 3 sec if no answer.
    	AdminClient admin = AdminClient.create(config);

    	//check if topic exists

		Set<String> topics = admin.listTopics().names().get(); // get set of all topics
    	for (String existingTopic : topics) {	
			//check if exists
    		if(topic.equals(existingTopic)) {
    			//topic exists, therefore we do not create a new 1.
    			return;
    		}
    	}
    	//if it does'nt exists, lets make one!
    	Map<String, String> configs = new HashMap<>();
    	int partitions = 1;
    	short replication = 3;
 	   	CreateTopicsResult result = admin.createTopics(Arrays.asList(new NewTopic(topic, partitions, replication).configs(configs)));	
		for (Map.Entry<String, KafkaFuture<Void>> entry : result.values().entrySet()) {
			entry.getValue().get();
		}
     }
    
    /**
     * preparing the message in the format requested.
     * @param topic - the name of the topic
     * @param message - the message of the topic
     * @return - right format of the message
     */
    private static String prepareMessage(String topic, String message){
    	LocalTime localTime = LocalTime.now();
    	String protocolMessage = "( " + topic + " ) " + LOGIN_NAME + " " + 
    								localTime.getHour()+":"+ localTime.getMinute()+":"+localTime.getSecond() + " - " + message;
    	return protocolMessage;
    }
    
    /**
     * choose a file from the "filesToSend" folder
     * @param path - the path of the folder.
     * @return - the chosen file to be sent
     */
    private static String chooseFile(String path) {
    	//folder dir
		File folder = new File(path);
		folder.mkdir();
		//get all "things" - files and folders
		File[] listOfThings = folder.listFiles();
		//get new list of files only.
		File[] listOfFiles = new File[listOfThings.length];
		int i = 0, answer = 0; // some play i did to handle wrong inputs.
		
		System.out.println("Choose a file to send(type number):");
		for (File file : listOfThings) {
		    if (file.isFile()) {
		    	listOfFiles[i++] = file;
		        System.out.println("("+i+"): " + file.getName());
		    }
		}
		if(i == 0) {
			System.out.println("The folder "+System.getProperty("user.dir")+"\\"+FILES_FOLDER+" is empty, nothing to send");
			return "";
		}
		//get a legal input of the file - the file index number
		do {
			try {
				if(answer > i) {
					System.out.println("Ilegal input! try again");
				}
				answer = Integer.parseInt(brIn.readLine().trim());
			} catch (NumberFormatException e) {
				System.out.println("Ilegal input! try again");
				answer = 0;
			} catch (IOException ioe) {
				System.out.println("Failed to read input: "+ ioe.getMessage());
				answer = 0;
			}
		}while(answer == 0 || answer > i);
		
		return listOfFiles[--answer].getName();
    }
    
    /**
     * print rules in the first encounter with the client, and in case illegal command was inserted.
     */
	private static void printRules(){
		System.out.println("Options Menu:\t[\"Join XXXXX\" to join the topic XXXXX]\t[\"XX message\" to send message to topic XX]\n"
				+ "\t\t[\"Leave XXXX\" to leave the topic XXXX]\t[\"Quit\"  to  finish and close the program]\n"
				+ "\t\t[\"Sendfile X\" to send file to topic X]\t[\"Getfile XXX\" to get file from topic XXX]");
	}
}