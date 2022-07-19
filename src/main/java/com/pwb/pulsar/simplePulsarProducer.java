package com.pwb.pulsar;

import java.io.IOException;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

public class simplePulsarProducer {

    private static final String SERVICE_URL = "pulsar+ssl://pulsar-gcp-uscentral1.streaming.datastax.com:6651";
	private static final String YOUR_PULSAR_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NTc5MTUzMDQsImlzcyI6ImRhdGFzdGF4Iiwic3ViIjoiY2xpZW50OzRhYmY1MzA4LWE1MTUtNGZhOS1iYWVjLTk0ZDE5M2U2ZjA5YztjSGRpWlhoaGJYQnNaUT09O2Y5NDAyYTU5MDEiLCJ0b2tlbmlkIjoiZjk0MDJhNTkwMSJ9.XjS7g_kGcnEuRnK4IeRazfP64YJZCZlAadqmQf3j2GQt11fMwzkIYSe0LUPzhTbbRYEOGIEULKvekjBLgnn2Wnx0Lhw-DS5yvYwLnFlL5fH3XefiYOK9E3yxH157kM1defVo18hVeRvLR_347KuldyRcyrObmSxrEswbak6ZfyfT3LsNRw1jLISo8OA_fells6M9OkUwwUVOtNvOzsJauXoN2pNuIZ-5KHnJQrN7F2kN2YAg6xGRjbWHSGP7h1QBi5sbV6h4jAcAfGI97eHOGUsLtR48B-oaJ34pdtRhnLMlPiuZDnaGYiue187e9LCk092Q2MhdQYLiNvA95ouKfg";

	public static void main(String[] args) throws IOException {
        // Create client object
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .authentication(
                    AuthenticationFactory.token(YOUR_PULSAR_TOKEN)
                )
                .build();

        // Create producer on a topic
        Producer<byte[]> producer = client.newProducer()
                .topic("persistent://pwbexample/iot-sensor/sensor-temps")
                .create();
        System.out.println("Producer created");
        // Send a message to the topic
        producer.send("Temp 100".getBytes());
        System.out.println("Message sent!");
        //Close the producer
        producer.close();
        System.out.println("Producer closed");
        // Close the client
        client.close();

    }

}
