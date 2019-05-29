package com.redhat;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaConfiguration;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.model.rest.RestBindingMode;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class FhirRoutes extends RouteBuilder {
	@Override
	public void configure() throws Exception {
		restConfiguration().component("servlet").bindingMode(RestBindingMode.json);
		
		KafkaComponent kafka = new KafkaComponent();		
		KafkaConfiguration kafkaConfig = new KafkaConfiguration();
		kafkaConfig.setBrokers("my-cluster-kafka-bootstrap-fhir-amq.apps.akrohg-openshift.redhatgov.io:443");
		kafkaConfig.setSecurityProtocol("SSL");
		kafkaConfig.setSslTruststoreLocation("src/main/resources/keystore.jks");
		kafkaConfig.setSslTruststorePassword("password");
		kafka.setConfiguration(kafkaConfig);
		
		getContext().addComponent("kafka", kafka);
		
		// "passthrough" kafka producer. Takes FHIR messages through RESTful API endpoint 
		// and forwards them directly to kafka, using the topic indicated by the resourceType
		rest("/rest")
			.post("/fhir").route().streamCaching().process(new Processor() {
				@Override
				public void process(Exchange exchange) throws Exception {
					Message message = exchange.getIn();
					String resourceType = message.getBody(Map.class).get("resourceType").toString().toLowerCase();
					
					log.info("Received FHIR message: {}", message.getBody(String.class));
					exchange.setProperty("topic", resourceType);
					log.info("Sending to kafka using topic: {}", resourceType);
					
					message.setBody(new ObjectMapper().writeValueAsString(exchange.getIn().getBody()));
					message.setHeader(KafkaConstants.PARTITION_KEY, 0);
					message.setHeader(KafkaConstants.KEY, "Camel");
				}
			}).recipientList(simple("kafka:${exchangeProperty.topic}")).setBody(constant("Message sent successfully."));
		
		
		// Consumer for messages which correspond to the "condition" kafka topic
		from("kafka:condition")
			.streamCaching()
			.log("Message received from Kafka : ${body}")
		    .log("    on the topic ${headers[kafka.TOPIC]}")
		    .log("    on the partition ${headers[kafka.PARTITION]}")
		    .log("    with the offset ${headers[kafka.OFFSET]}")
		    .log("    with the key ${headers[kafka.KEY]}")
		    .unmarshal().json(JsonLibrary.Jackson, Map.class)
		    .log("Initiating PAM task to handle condition for patient ${body[subject][reference]}");
		    
	}
}
