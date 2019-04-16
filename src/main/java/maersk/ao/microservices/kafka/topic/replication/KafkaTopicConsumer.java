package maersk.ao.microservices.kafka.topic.replication;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import maersk.ao.microservices.kafka.topic.configurations.KafkaConsumerConfigurations;


@Component
@EnableKafka
public class KafkaTopicConsumer {

    static Logger log = Logger.getLogger(KafkaTopicConsumer.class);

    @Value("${kafka.debug:false}")
    private boolean _debug;
    private KafkaTemplate<String, String> kafkaProducerTemplate;
    
    //@Autowired
    private KafkaTopicProducer kafkaTopicProducer;
    
	private KafkaConsumerConfigurations kafkaConsumerConfig;
	//private KafkaProducerConfigurations kafkaProducerConfig;

	public KafkaTopicConsumer(KafkaTopicProducer ktp) {
		this.kafkaTopicProducer = ktp;
	}
	
	//@KafkaListener(topicPartitions = @TopicPartition(topic = "${kafka.src.topic}", 
	//		partitionOffsets = @PartitionOffset(initialOffset = "0", partition = "0")) )
    /*
	@KafkaListener(topics = "${kafka.src.topic}", containerFactory="kafkaListenerContainerFactory")
	public void listen(ConsumerRecord<?,?> consumerRecord, Acknowledgment ack) {
        System.out.println("received message on " 
        					+ consumerRecord.topic() 
        					+ "- key:" 
        					+ consumerRecord.key()
        					+ " value: " + consumerRecord.value());

        ack.acknowledge();
        
    }
	*/

	@KafkaListener(topics = "${kafka.src.topic}", containerFactory="kafkaListenerContainerFactory")
	public void listen(ConsumerRecord<?,?> consumerRecord, KafkaConsumer consumer, Acknowledgment ack) throws InterruptedException, ExecutionException {

		if (this._debug) { log.info("listen - ack mode");
			log.info("AckMode: received message on " 
					+ consumerRecord.topic() 
					+ "- key   :" + consumerRecord.key()
					+ "- offset: " + consumerRecord.offset()
					+ "- partition: " + consumerRecord.partition()
					+ "- value : " + consumerRecord.value());
		}

		if (ack != null) {
	        if (this._debug) { log.info("KafkaTopicConsumer: AckMode: Calling kafkaTopicProducer.send()"); }         
	        if (this.kafkaTopicProducer != null) {
	        	this.kafkaTopicProducer.send(consumerRecord.key(), consumerRecord.value().toString());
	        } else {
	            log.warn("KafkaTopicConsumer: AckMode: kafkaTopicProducer is null"); 
	        }
			ack.acknowledge();
		//	consumer.commitAsync();
		} else {
			log.warn("AckMode: Acknowledgement is null"); 
		}
		
        
	}
	
	@KafkaListener(topics = "${kafka.src.topic}", containerFactory="kafkaListenerContainerFactory")
	public void listen(ConsumerRecord<?,?> consumerRecord, KafkaConsumer consumer) throws InterruptedException, ExecutionException {

		if (this._debug) { 
			log.info("listen - NO ack mode");
			log.info("NoAck: received message on " 
					+ consumerRecord.topic() 
					+ "- key   :" + consumerRecord.key()
					+ "- offset: " + consumerRecord.offset()
					+ "- partition: " + consumerRecord.partition()
					+ "- value : " + consumerRecord.value());
		
		}

        System.out.println("KafkaTopicConsumer: NoAck: Calling kafkaTopicProducer.send()");         
        if (this.kafkaTopicProducer != null) {
        	this.kafkaTopicProducer.send(consumerRecord.key(), consumerRecord.value().toString());
        } else {
           log.warn("KafkaTopicConsumer: NoAck: kafkaTopicProducer has not been created"); 
        }
        //ProducerRecord<String, String> record =  
        //		new ProducerRecord<>(kafkaProducerConfig.GetDestTopic(), consumerRecord.value().toString());

        //RecordMetadata recordMetadata = kafkaProducerTemplate.send(record).get().getRecordMetadata();

        //kafkaProducerTemplate.send(record);

        consumer.commitAsync();
    }

	/*
    @KafkaListener(topics = {"${kafka.src.topic}"}, containerFactory = "kafkaListenerContainerFactory")
    public void replicateMessage(@Payload String message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY)
                                 Acknowledgment acknowledgment) throws ExecutionException, InterruptedException  {


            ProducerRecord<String, String> record =  new ProducerRecord<>(kafkaConfig.getDestTopic(), message);

            RecordMetadata recordMetadata = kafkaTemplate.send(record).get().getRecordMetadata();

            log.debug("Message key : {} - Published to Kafka topic {} on partition {} at offset {}. result message: {}",
                     kafkaConfig.getDestTopic(), recordMetadata.partition(), recordMetadata.offset(), message);

            acknowledgment.acknowledge();
       }
		*/
	
}
