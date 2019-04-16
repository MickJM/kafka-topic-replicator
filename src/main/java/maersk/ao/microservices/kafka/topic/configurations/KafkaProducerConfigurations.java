package maersk.ao.microservices.kafka.topic.configurations;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;

import maersk.ao.microservices.kafka.topic.replication.KafkaTopicProducer;

@Controller
public class KafkaProducerConfigurations {

	// logger
    static Logger log = Logger.getLogger(KafkaProducerConfigurations.class);

    @Value("${kafka.debug:false}")
    private boolean _debug;

    //*******************************
    // producer property
    @Value("${kafka.dest.bootstrap.servers}")
    private String destBootstrapServers;
    @Value("${kafka.dest.username:}")
    private String destUsername;
    @Value("${kafka.dest.password:}")
    private String destPassword;
    @Value("${kafka.dest.login.module:org.apache.kafka.common.security.plain.PlainLoginModule}")
    private String destLoginModule;
    @Value("${kafka.dest.sasl.mechanism:PLAIN}")
    private String destSaslMechanism;
    @Value("${kafka.dest.truststore.location:}")
    private String destTruststoreLocation;
    @Value("${kafka.dest.truststore.password:}")
    private String destTruststorePassword;
    @Value("${kafka.dest.linger:1}")
    private int destLinger;
    @Value("${kafka.dest.timeout:30000}")
    private int destRequestTimeout;
    @Value("${kafka.dest.timeout:60000}")
    private int destTransactionTimeout;
    @Value("${kafka.dest.batch.size:16384}")
    private int destBatchSize;
    @Value("${kafka.dest.send.buffer:131072}")
    private int destSendBuffer;
    @Value("${kafka.dest.acks.config:all}")
    private String destAcksConfig;
    @Value("${kafka.dest.clientId}")
    private String destClientId;
    @Value("${kafka.dest.security.protocol:SASL_SSL}")
    private String destSecurityProtocol;
    @Value("${kafka.dest.topic}")
    private String destTopic;


	/*
	@Bean
	public Map<String, Object> producerProperties() {
		
		if (this._debug) { System.out.println("ProducerFactory: started"); }    		
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destBootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, destLinger);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, destRequestTimeout);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, destBatchSize);
        properties.put(ProducerConfig.SEND_BUFFER_CONFIG, destSendBuffer);
        properties.put(ProducerConfig.ACKS_CONFIG, destAcksConfig);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, destClientId);

		if (this._debug) { System.out.println("ProducerFactory: SASL"); }    		
        addSaslProperties(properties, destSaslMechanism, destSecurityProtocol, destLoginModule, destUsername, destPassword);

		if (this._debug) { System.out.println("ProducerFactory: truststore"); }    		
        addTruststoreProperties(properties, destTruststoreLocation, destTruststorePassword);
        return properties;
	}
	*/
	
    @Bean
    public ProducerFactory<String, String> producerFactory() {
    	
    	log.info("ProducerFactory: started");	
        
		Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.destBootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, this.destLinger);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, this.destRequestTimeout);
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, this.destTransactionTimeout);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, this.destBatchSize);
        properties.put(ProducerConfig.SEND_BUFFER_CONFIG, this.destSendBuffer);
        properties.put(ProducerConfig.ACKS_CONFIG, this.destAcksConfig);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, this.destClientId);

		if (this._debug) { 
			log.info("ProducerFactory: TopicName:" + this.destTopic);    
			log.info("ProducerFactory: SASL"); } 
    
        addSaslProperties(properties, destSaslMechanism, destSecurityProtocol, destLoginModule, destUsername, destPassword);

		if (this._debug) { log.info("ProducerFactory: truststore"); }    		
        addTruststoreProperties(properties, destTruststoreLocation, destTruststorePassword);
        
        return new DefaultKafkaProducerFactory<>(properties);
    }

    private void addSaslProperties(Map<String, Object> properties, String saslMechanism, String securityProtocol, String loginModule, String username, String password) {

		if (this._debug) { log.info("addSaslProperties: started"); }    		

    	if (!StringUtils.isEmpty(username)) {
            properties.put("security.protocol", securityProtocol);
            properties.put("sasl.mechanism", saslMechanism);
            String saslJaaSConfig = String.format("%s required username=\"%s\" password=\"%s\" ;", loginModule, username, password);
            properties.put("sasl.jaas.config", saslJaaSConfig);
    		if (this._debug) { log.info("addSaslProperties: security set"); }    		
        }
		if (this._debug) { log.info("addSaslProperties: exit"); }    		

    }

    /***
     * Add TLS Truststore
     * 
     * @param properties
     * @param location
     * @param password
     */
    private void addTruststoreProperties(Map<String, Object> properties, String location, String password) {
		
    	if (this._debug) { log.info("addTruststoreProperties: start"); }    		
    	if (!StringUtils.isEmpty(location)) {
        	if (this._debug) { log.info("addTruststoreProperties: truststore set"); }    		
            properties.put("ssl.truststore.location", location);
            properties.put("ssl.truststore.password", password);
        }
    	if (this._debug) { log.info("addTruststoreProperties: exit"); }    		
    	
    }

    @Bean
    public KafkaTemplate<String, String> kafkaProducerTemplate() {
    	if (this._debug) { log.info("KafkaProducerConfigurations: Creating KafkaProducerTemplate"); }    	
    	return new KafkaTemplate<String,String>(producerFactory());
    }
    
    /*
    @Bean
    public KafkaProducer<String,String> kafkaProducer() {
    	System.out.println("KafkaProducerConfigurations: Creating KafkaProducer");
		return new KafkaProducer<String,String>(producerProperties());
    }
    */
    
    @Bean
    public KafkaTopicProducer sender() {
    	if (this._debug) { log.info("KafkaProducerConfigurations: Creating KafkaTopicProducer"); }    	
    	return new KafkaTopicProducer();
    }
    
    
}
