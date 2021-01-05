/*
 * The MIT License
 *
 * Copyright 2021 randalkamradt.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package net.kamradtfamily.usedvehicles;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import static net.kamradtfamily.usedvehicles.TruckConsumer.reactiveCollection;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.SenderOptions;

/**
 *
 * @author randalkamradt
 */
public class CarConsumer {
    private static final String CAR_QUEUE_NAME = "car-queue";
    private static final String HOST_NAME = "localhost";
    private static final int PORT = 5672;
    private static final String USER_NAME = "guest";
    private static final String PASSWORD = "guest";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    static final ObjectReader carReader = objectMapper.readerFor(Vehicle.Car.class);
    static final Cluster cluster = Cluster.connect("127.0.0.1", "admin", "admin123");
    static final Bucket bucket = cluster.bucket("po");
    static final Collection collection = bucket.defaultCollection();
    static final ReactiveCollection reactiveCollection = collection.reactive();
    
    public static void consume() {
        ConnectionFactory cfactory = new ConnectionFactory();
        cfactory.setHost(HOST_NAME);
        cfactory.setPort(PORT);
        cfactory.setUsername(USER_NAME);
        cfactory.setPassword(PASSWORD);
        SenderOptions soptions = new SenderOptions()
                .connectionFactory(cfactory);
        Sender sender = RabbitFlux.createSender(soptions);
        ReceiverOptions roptions = new ReceiverOptions()
                .connectionFactory(cfactory);
        Receiver carReceiver = RabbitFlux.createReceiver(roptions);
        carReceiver
            .consumeAutoAck(CAR_QUEUE_NAME)
            .delaySubscription(sender.declareQueue(QueueSpecification.queue(CAR_QUEUE_NAME)))
            .timeout(Duration.ofSeconds(10))
            .doFinally((s) -> {
                log("Car consumer in finally for signal " + s);
                carReceiver.close();
                sender.close();
            })
            .map(j -> readCarJson(new String(j.getBody())))
            .flatMap(o -> Mono.justOrEmpty(o))
            .flatMap(v -> reactiveCollection
                    .get(v.getPo().getId())
                    .doOnNext(j -> log("po for car " + v.getPo().getId() + " confirmed"))
                    .map(j -> v)
                    .single()
                    .onErrorReturn(v))
            .map(c -> new Vehicle.Car(c.getPo(),"car lot a"))
            .subscribe(c -> log("received car " + c));
        
    }
    
    private static void log(String msg) {
        System.out.println(Thread.currentThread().getName() + " " + msg);
    }
    
    private static Optional<Vehicle.Car> readCarJson(String car) {
        try {
            return Optional.of(carReader.readValue(car));
        } catch (JsonProcessingException ex) {
            log("unable to serialize car");
            ex.printStackTrace(System.out);
            return Optional.empty();
        } catch (IOException ex) {
            log("unable to serialize car");
            ex.printStackTrace(System.out);
            return Optional.empty();
        }
    }
    
}
