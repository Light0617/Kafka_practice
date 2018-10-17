package com.github;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class UserDataProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        Properties config = new Properties();
        // kafka bootstrap server
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // producer acks
        config.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        config.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        config.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

        // leverage idempotent producer from Kafka 0.11 !
        config.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates
        Producer<String, String> producer = new KafkaProducer<>(config);

        //1. we create a new user
        System.out.println("first new user");
        //using get() to force future completed and then execute the next one
        producer.send(userRecord("john", "First=John,Last=Doe,Email=john.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Apples and Bananas (1)")).get();
        Thread.sleep(10000);

        //2. we receive user purchase, but the user is not in Kafka
        System.out.println("non-existing user");
        producer.send(purchaseRecord("bob", "Kafka Udemy Course (2)")).get();
        Thread.sleep(10000);

        //3. we update user "john", and add a new transaction
        System.out.println("update the user");
        producer.send(purchaseRecord("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Oranges (3)")).get();
        Thread.sleep(10000);

        //4. we send a user purchase for tony, but it exists in Kafka later
        System.out.println("\nExample 4 - non existing user then user\n");
        producer.send(purchaseRecord("tony", "Computer (4)")).get();
        producer.send(userRecord("tony", "First=Tony,Last=Smith,GitHub=simpletony")).get();
        producer.send(purchaseRecord("tony", "Books (4)")).get();
        producer.send(userRecord("tony", null)).get(); // delete for cleanup
        Thread.sleep(10000);

        // 5 - we create a user, but it gets deleted before any purchase comes through
        System.out.println("\nExample 5 - user then delete then data\n");
        producer.send(userRecord("alice", "First=Alice")).get();
        producer.send(userRecord("alice", null)).get(); // it will delete record
        producer.send(purchaseRecord("alice", "Apache Kafka Series (5)")).get();
        Thread.sleep(10000);

        System.out.println("End of demo");
        producer.close();
    }

    private static ProducerRecord<String, String> userRecord(String key, String value) {
        return new ProducerRecord<>("user-table", key, value);
    }

    private static ProducerRecord<String, String> purchaseRecord(String key, String value) {
        return new ProducerRecord<>("user-purchase", key, value);
    }
}
