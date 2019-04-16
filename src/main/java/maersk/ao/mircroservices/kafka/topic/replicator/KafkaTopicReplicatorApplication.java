package maersk.ao.mircroservices.kafka.topic.replicator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "maersk.ao.microservices.kafka.topic.configurations")
@ComponentScan(basePackages = "maersk.ao.microservices.kafka.topic.replication")
public class KafkaTopicReplicatorApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaTopicReplicatorApplication.class, args);
	}

}
