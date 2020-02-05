package kaf.ka.eran.sasha;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class ProducerFileClass extends Thread{
	private String topic, fileName ,FILE_PATH;
	Producer<String, byte[]> producer;
	
	public ProducerFileClass(String topic, String fileName, String brokerIP){
		this.topic = topic;
		this.fileName = fileName;
		this.FILE_PATH = "filesToSend\\" + fileName;
		//initialize producer
		producer = createProducer(brokerIP);
   	}

	/**
	 * producer creation function, setting properties and returning the producer.
	 * @return the file producer instance
	 */
    private Producer<String, byte[]> createProducer(String broker) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker); //broker ip
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "FileProducer_" + LocalDateTime.now()); //must be unique\
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //key
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()); //value
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000); //max delay of sending before exception error (3 sec).
        return new KafkaProducer<>(properties);
    }
    
    @Override
	public void run(){
        //make the record to be sent from the producer to the broker
    	//ProducerRecord<Long, byte[]> record = new ProducerRecord<>(topic, key, message);   
 		byte[] fileData;
 		ArrayList<byte[]> blocksOf10Ks;
 		Path path = Paths.get(FILE_PATH);
 		
		try {
			//read the entire file
			fileData = Files.readAllBytes(path);
			//split it to blocks of 10kbs
			blocksOf10Ks = splitFileTo10Ks(fileData);
			
			//send all blocks
			for (byte[] bs : blocksOf10Ks) {
				ProducerRecord <String, byte[]> data =new ProducerRecord<>(topic, fileName, bs);
		 		producer.send(data);
			}
			System.out.println("File sent!");
		} catch (IOException e) {
			System.out.println("Error in reading a file, file doesnt exists in: "+  System.getProperty("user.dir") + e.getMessage());
		} catch (Exception e) {
			System.out.println("Error in file producer: " + e.getMessage());
		}

    }
    
	/**
	 * Internet suggested that the optimal kafka message size is 10KB so we split it to 10Ks
	 * @param fileData - the bytes of data of the file we are splitting
	 * @return - return the data split to arraylist of data.
	 */
 	private ArrayList<byte[]> splitFileTo10Ks(byte[] fileData)
 	{
 		int fileLength = fileData.length;
 		int blockSize = 10240; 	//10k
 		int numOfBlocks = fileLength / blockSize; //full block sizes - what's left will come later

 		int indexer = 0;
 		//the array list to hold all the 10KB blocks
 		ArrayList<byte[]> data=new ArrayList<byte[]>();
 		
 		//add to the data list all the blocks
 		for (int i = 0; i < numOfBlocks; i++)
 		{
 			data.add(Arrays.copyOfRange(fileData, indexer, indexer+blockSize));
 			indexer += blockSize;
 		}
 		//add what's left "extra" from what's left that is less than the block size
 		data.add(Arrays.copyOfRange(fileData, indexer, fileLength));
 		//mark the end of file
 		data.add(null);
 		return data;
 	}
}
