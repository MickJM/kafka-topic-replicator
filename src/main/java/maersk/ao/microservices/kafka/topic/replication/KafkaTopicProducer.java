package maersk.ao.microservices.kafka.topic.replication;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import maersk.ao.microservices.kafka.topic.configurations.KafkaProducerConfigurations;
//import maersk.ibm.message.hub.v1.MessageHubProducerCallBack;


public class KafkaTopicProducer {

    private static Logger log = Logger.getLogger(KafkaTopicProducer.class);

    @Value("${kafka.debug:false}")
    private boolean _debug;

    @Value("${kafka.dest.topic}")
    private String destTopic;

    @Autowired
    private KafkaTemplate<String, String> kafkaProducerTemplate;
                                          
    //@Autowired
    //private KafkaProducer<String, String> kafkaProducer;
	private KafkaProducerConfigurations kafkaProducerConfig;
	
	// Send with a key
    public void send(Object key, String payload)  {
        
    	if (this._debug) { 
    		log.info("Sending payload ");
    		log.info("ProducerFactory: TopicName:" + this.destTopic); }    		

        //ProducerRecord<String, String> record =  
        //		new ProducerRecord<>(kafkaProducerConfig.GetDestTopic(), consumerRecord.value().toString());

    	if (this._debug) { log.info("KafkaTopicProducer: Creating ProducerRecord"); }
        ProducerRecord<String, String> record = null;        
        try {
         	// If a key is supplied, then use it ...
        	if (key != null) {
        		record = new ProducerRecord<String, String>(this.destTopic, key.toString(), payload);
        	} else {
        		record = new ProducerRecord<String, String>(this.destTopic, payload);
        	}
        	
    		if (this._debug) { log.info("ProducerFactory: Producer record created ..."); }    		

        } catch (Exception e) {
            log.error("KafkaTopicProducer: Error creating Kafka ProducerRecord");        	
            log.error("KafkaTopicProducer: " + e.getMessage());        	
            System.exit(0);
        }

        if (record != null) {
    		if (this._debug) { log.info("ProducerFactory: sending ..."); }    		
    		kafkaProducerTemplate.send(record);
    		/*
    		ListenableFuture<SendResult<String, String>> future = kafkaProducerTemplate.send(record);
    		future.addCallback(null, new ListenableFutureCallback<SendResult<String, String>>() {

				@Override
				public void onSuccess(SendResult<String, String> result) {
					// TODO Auto-generated method stub
					
				}

				@Override
				public void onFailure(Throwable ex) {
					// TODO Auto-generated method stub
					
				}

    		});
    		*/
    		
    		if (this._debug) { log.info("ProducerFactory: message sent"); }    		

    		//kafkaProducer.send(record);

    		//, new MessageHubProducerCallBack());		        	
        } else {
            log.warn("KafkaTopicProducer: producer record is null");        	
        }
        
        //System.out.println("KafkaTopicProducer: Creating RecordMetadata");
        /*
        try {
			RecordMetadata recordMetadata = 
					kafkaProducerTemplate.send(record).get().getRecordMetadata();
	        System.out.println("KafkaTopicProducer: message created");
	
        } catch (InterruptedException e) {
            System.out.println("KafkaTopicProducer: InterruptedException : " 
            			+ e.getMessage());

		} catch (ExecutionException e) {
            System.out.println("KafkaTopicProducer: ExecutionException : " 
        			+ e.getMessage());
		}
		*/
        //kafkaProducerTemplate.send("helloworld.t", payload);
      }
	
}
