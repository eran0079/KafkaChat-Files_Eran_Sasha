package kaf.ka.eran.sasha;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

//the consumer class that runs in a thread for each topic
public class ConsumerClass extends Thread {

	private String MESSAGE = "message";
	private Consumer<String, String> consumer;
	//get the consumer instance
	public ConsumerClass(Consumer<String, String> consumer) {
		this.consumer = consumer;
	}
	
	@Override
	public void run(){
	        ConsumerRecords<String, String> consumerRecords;
	        try{
		        while (true) {
		        	//get 10 records at a time.
		            consumerRecords = consumer.poll(10);
		            // check if empty - do nothing
		            if (consumerRecords.count()==0) {
		                Thread.sleep(100);
		                continue;
		            }
		            //read the records
		            consumerRecords.forEach(record -> {
		            	if(record.key().startsWith(MESSAGE)){
			                System.out.println(record.value());
		            	}		            	
			        });
		            //update the offset
			        consumer.commitSync();
		        }
	        }
	        catch (WakeupException we) {//we are closing the consumer
				consumer.close(); 
	        }
	        catch (Exception e) {
				System.out.println("Error in consumer: " + e.getMessage());
				e.printStackTrace();
				consumer.close();
	        }
	    }
}
