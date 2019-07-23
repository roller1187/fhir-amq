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
		
//		KafkaComponent kafka = new KafkaComponent();			
//		getContext().addComponent("kafka", kafka);
		
		// "passthrough" kafka producer. Takes FHIR messages through RESTful API endpoint 
		// and forwards them directly to kafka, using the topic indicated by the resourceType
		rest("/rest")
			.post("/fhir").route().streamCaching().process(new Processor() {
				@Override
				public void process(Exchange exchange) throws Exception {
					String kafkaTopic = "my-topic";
					Message message = exchange.getIn();
					
					log.info("Received FHIR message: {}", message.getBody(String.class));
					exchange.setProperty("topic", kafkaTopic);
					log.info("Sending to kafka using topic: {}", kafkaTopic);
					
					message.setBody(new ObjectMapper().writeValueAsString(exchange.getIn().getBody()));
					message.setHeader(KafkaConstants.PARTITION_KEY, 0);
					message.setHeader(KafkaConstants.KEY, "Camel");
				}
			}).recipientList(simple("kafka:${exchangeProperty.topic}")).setBody(constant("Message sent successfully."));    
		
		from("timer://foo?period=10s").to("http://localhost:8080/randomMessage/xml").log(simple("${body}").toString());
	}
}
