package com.nilay.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerApp {

    public static void main (String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String,String>(props);

        try{
                for(int i = 0 ; i<150 ; i++){
                    kafkaProducer.send(new ProducerRecord<String, String>("nilay_topic", Integer.toString(i),"My Message " + Integer.toString(i)) );
                }
        }catch (Exception e){
            System.out.println(e.getStackTrace());
        }
        finally {
            kafkaProducer.close();
        }
    }
}
