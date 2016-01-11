package ly.stealth.kafkahttp;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.Strings;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.nio.charset.Charset;
import java.util.*;

@Path("/message")
@Produces(MediaType.APPLICATION_JSON)
public class MessageResource {
    private KafkaProducer producer;
    private Properties consumerCfg;

    private static final String TOPIC = "caliper_events";

    public MessageResource(KafkaProducer producer, Properties consumerCfg) {
        this.producer = producer;
        this.consumerCfg = consumerCfg;
    }

    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Response produce(
            String message
    ) {
        List<String> errors = new ArrayList<>();
        if (Strings.isNullOrEmpty(message)) errors.add("Undefined message");

        if (!errors.isEmpty())
            return Response.status(400)
                    .entity(errors)
                    .build();

        Charset charset = Charset.forName("utf-8");
        System.out.println("Calling Kafka: " + message);
        producer.send(new ProducerRecord(TOPIC, null, message.getBytes(charset)), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    System.out.println("[Kafka] Failed:");
                    e.printStackTrace();
                } else {
                    System.out.println("[Kafka] Success!");
                }
            }
        });


        return Response.ok()
                .header("Access-Control-Allow-Origin", "*")
                .build();
    }

    @GET
    @Timed
    public Response consume(
            @QueryParam("timeout") Integer timeout
    ) {
        if (Strings.isNullOrEmpty(TOPIC))
            return Response.status(400)
                    .entity(new String[]{"Undefined topic"})
                    .build();

        Properties props = (Properties) consumerCfg.clone();
        if (timeout != null) props.put("consumer.timeout.ms", "" + timeout);

        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerConnector connector = Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> streamCounts = Collections.singletonMap(TOPIC, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(streamCounts);
        KafkaStream<byte[], byte[]> stream = streams.get(TOPIC).get(0);

        List<Message> messages = new ArrayList<>();
        try {
            for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream)
                messages.add(new Message(messageAndMetadata));
        } catch (ConsumerTimeoutException ignore) {
        } finally {
            connector.commitOffsets();
            connector.shutdown();
        }

        return Response.ok(messages)
                .build();
    }

    @OPTIONS
    public Response corsHandler() {
        return Response.ok()
                .header("Access-Control-Allow-Credentials", "false")
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST")
                .header("Access-Control-Allow-Headers", "Content-Type,Access-Control-Allow-Origin")
                .build();
    }

    public static class Message {
        public String topic;

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String key;
        public String message;

        public int partition;
        public long offset;

        public Message(MessageAndMetadata<byte[], byte[]> message) {
            this.topic = message.topic();

            this.key = message.key() != null ? new String(message.key(), Charset.forName("utf-8")) : null;
            this.message = new String(message.message(), Charset.forName("utf-8"));

            this.partition = message.partition();
            this.offset = message.offset();
        }
    }
}
