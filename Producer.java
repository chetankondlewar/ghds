package Kafka.kafkaPC;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer 
{
    public static void main( String[] args )
    {
       // Steps
    	// 1. Create the producer Configration
    	  String server="127.0.0.1:9092";
    	  Properties ProducerProp=new Properties();
    	  ProducerProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
    	  ProducerProp.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	  ProducerProp.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	  
    	// 2. create Apache kafka Producer
    	  String topic="test_1";
    	  String value="Producer Welcome into kafka new Topic test_1 new data add1123";
    	  
    	  KafkaProducer<String, String> myProducer=new KafkaProducer<String, String>(ProducerProp);
    	// 3. 	 Prepare data for Producer to send
    	  ProducerRecord<String, String> myrecord=new ProducerRecord<String, String>(topic, value);
    	// 4. sent the data to the topic
    	 myProducer.send(myrecord);
    	// 5. closed the data post sentBoss.m1()
    	 myProducer.flush();
    	// 6. closed the Producer
    	 myProducer.close();
    }
}
