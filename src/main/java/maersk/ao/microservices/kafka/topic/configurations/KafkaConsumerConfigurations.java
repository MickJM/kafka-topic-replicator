package maersk.ao.microservices.kafka.topic.configurations;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.util.StringUtils;

//import org.springframework.kafka.core.ConsumerFactory;
//import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.common.serialization.StringDeserializer;

@Configuration
public class KafkaConsumerConfigurations {

	// logger
    static Logger log = Logger.getLogger(KafkaConsumerConfigurations.class);

    @Value("${kafka.debug:false}")
    private boolean _debug;

    //consumer property
    @Value("${kafka.src.bootstrap.servers}")
    private String srcBootstrapServers;
    @Value("${kafka.src.username}")
    private String srcUsername;
    @Value("${kafka.src.password}")
    private String srcPassword;
    @Value("${kafka.src.login.module:org.apache.kafka.common.security.plain.PlainLoginModule}")
    private String srcLoginModule;
    @Value("${kafka.src.sasl.mechanism:PLAIN}")
    private String srcSaslMechanism;
    @Value("${kafka.src.truststore.location:}")
    private String srcTruststoreLocation;
    @Value("${kafka.src.truststore.password:}")
    private String srcTruststorePassword;
    @Value("${kafka.src.consumer.group:replicator}")
    private String srcConsumerGroup;
    @Value("${kafka.src.offset.auto.reset:earliest}")
    private String srcOffsetAutoReset;
    @Value("${kafka.src.max.poll.records:500}")
    private String srcMaxPollRecords;
    @Value("${kafka.src.topic}")
    private String sourceTopic;
    @Value("${kafka.src.security.protocol:SASL_SSL}")
    private String srcSecurityProtocol;
    @Value("${kafka.src.clientId}")
    private String srcClientId;
    @Value("${kafka.src.concurrency:3}")
    private int srcConcurrency;
    @Value("${kafka.src.retry.max.attempts:3}")
    private int maxRetryAttempts;
    @Value("${kafka.src.retry.initial.interval-secs:1}")
    private int retryInitialIntervalSeconds;
    @Value("${kafka.src.consumer.retry.max.interval.secs:10}")
    private int retryMaxIntervalSeconds;

	@Value("${kafka.application.concurrency:1}")
	private int concurrency;
    
	
	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> 
							kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {
        
		if (this._debug) { log.info("KafkaListenerContainerFactory: Start"); }
		
		ConcurrentKafkaListenerContainerFactory<String, String> factory 
        			= new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(this.concurrency);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);        

        //factory.setStatefulRetry(true);
        
		if (this._debug) { log.info("KafkaListenerContainerFactory: return"); }

        return factory;
    }
    
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {

		log.info("**********************************************"); 
		log.info("ConsumerFactory being created"); 

        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.srcBootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, this.srcConsumerGroup);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.srcOffsetAutoReset);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.srcMaxPollRecords);

		if (this._debug) { log.info("ConsumerFactory: setting SASL"); }    		
        addSaslProperties(properties, this.srcSaslMechanism, this.srcSecurityProtocol, 
        		this.srcLoginModule, this.srcUsername, this.srcPassword);

		if (this._debug) { log.info("ConsumerFactory: setting truststore"); }    		
        addTruststoreProperties(properties, this.srcTruststoreLocation, this.srcTruststorePassword);
    	return new DefaultKafkaConsumerFactory<>(properties);
    }

    /***
     * Add SASL properties
     * 
     * @param properties
     * @param saslMechanism
     * @param securityProtocol
     * @param loginModule
     * @param username
     * @param password
     */
    private void addSaslProperties(Map<String, Object> properties, String saslMechanism, String securityProtocol, String loginModule, String username, String password) {

		if (this._debug) { System.out.println("addSaslProperties: started"); }    		

    	if (!StringUtils.isEmpty(username)) {
            properties.put("security.protocol", securityProtocol);
            properties.put("sasl.mechanism", saslMechanism);
            String saslJaaSConfig = String.format("%s required username=\"%s\" password=\"%s\" ;", loginModule, username, password);
            properties.put("sasl.jaas.config", saslJaaSConfig);
            
    		if (this._debug) { 
    			log.info("addSaslProperties: security set"); 
    			log.info("saslJaasConfig : " + saslJaaSConfig );
    			log.info("addSaslProperties: security set"); 
    		}    		
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


}
