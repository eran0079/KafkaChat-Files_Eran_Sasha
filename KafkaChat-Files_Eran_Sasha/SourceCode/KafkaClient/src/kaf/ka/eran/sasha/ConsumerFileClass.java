package kaf.ka.eran.sasha;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerFileClass extends Thread{
	private Consumer<String, byte[]> consumer;
	private String topic;
	private String MESSAGE = "message";
	private String FILE_PATH = "filesToReceive\\";
	
	public ConsumerFileClass(String topic, String brokerIP){
		this.topic = topic;
		consumer = createConsumer(topic, brokerIP);
		File makeFolder = new File(FILE_PATH);
		makeFolder.mkdir();
	}
	
	@Override
	public void run(){
		try{			
			//memory to hold the bytes of the received file
			ByteArrayOutputStream memoryStorage = new ByteArrayOutputStream();
			String fileName = "";
			boolean gotFileName = false, emptyFolder = true; //tricks to manage different outputs
			
			while (true) {
				//poll for new kafka records
				ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(1000);
				//check if the topic is empty, nothing to do
	            if (consumerRecords.count() == 0) {
	            	if(emptyFolder) {
	            		System.out.println("No file in topic: " + this.topic);
	            	}
	            	else {
	            		System.out.println("No more files in topic: " + this.topic);
	            	}
	            	return;
	            }
	            //iterate over all records and save them to byteArray.
				for (ConsumerRecord<String, byte[]> record : consumerRecords)
				{
					//check if its a file, if not quit
					if(record.key().equals(MESSAGE)){
						continue;
					}
					//get the file name, once only is enough.
					if(!gotFileName){
						fileName = record.key();
						//validate file name
						fileName = getFileName(fileName);
						//don't check anymore
						gotFileName = true;
					}
					// check if file finished.
					if (record.value() != null) //if not finished add to the memory
					{
						memoryStorage.write(record.value()); 
					}
					else  	//if finished write it to the project folder
					{	
						String filePath = FILE_PATH + fileName;
						writeFile(filePath, memoryStorage.toByteArray());
						memoryStorage.reset();
						System.out.println("Finished downloading file: " + fileName);
						gotFileName = false;
						emptyFolder = false;
					}
				}
			}
		}
		catch(IOException ioe){
			System.out.println("Error in file writer of the consumer: " + ioe.getMessage());
		}
		catch(Exception e){
			System.out.println("Error in file consumer: " + e.getMessage());
		}
	}
	
	/**
	 * write the file to the filesToReceive folder
	 * @param filePath - the name says it all
	 * @param dataBytes - the bytes of the memory we saved the file blocks to
	 * @throws IOException - well if we fail. bummer ain't it?
	 */
	private void writeFile(String filePath, byte[] dataBytes) throws IOException
	{ 
		FileOutputStream writer = new FileOutputStream(filePath);
		writer.write(dataBytes);
		writer.flush();
		writer.close();  

	}
	
	/**
	 * manage copy of files with the same name in the folder
	 * @param filename - the file name we are checking
	 * @return - the new file name after verifying its not a used name
	 */
	public String getFileName(String filename){
		File file = new File(FILE_PATH + filename);
	    int num = 0;
	    String newFileName = "";
	    //seperate the file name and the file ending
		int i = filename.lastIndexOf('.');
		String[] a =  {filename.substring(0, i), filename.substring(i)};
		
		//check if file exists
	    if (file.exists() && !file.isDirectory()) {
	        //rename the file name until it does'nt exists.
	    	while(file.exists()){
	        	num++;
	        	newFileName = a[0]+ "(" + num + ")" + a[1];
	        	file = new File(FILE_PATH + newFileName);
	        }
	    } 
	    else {
	        newFileName = filename;
	    }
	    return newFileName;
	}
	
	/**
	 * set the properties of the file consumer
	 * @param topic - the topic it will read from
	 * @param broker - the broker IP we are connected to
	 * @return - the file consumer instance
	 */
    private Consumer<String, byte[]> createConsumer(String topic, String broker) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker); //broker ip
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer_" + LocalDateTime.now()); // must be unique
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //key
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()); //value
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }
}



