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
import com.fasterxml.jackson.databind.ObjectWriter;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.OutboundMessageResult;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.SenderOptions;

/**
 *
 * @author randalkamradt
 */
public class PurchaseOrderConsumer {
    private static final String PO_QUEUE_NAME = "po-queue";
    private static final String CAR_QUEUE_NAME = "car-queue";
    private static final String TRUCK_QUEUE_NAME = "truck-queue";
    private static final String MOTORCYCLE_QUEUE_NAME = "motorcycle-queue";

    private static final String HOST_NAME = "localhost";
    private static final int PORT = 5672;
    private static final String USER_NAME = "guest";
    private static final String PASSWORD = "guest";
    private static final Cluster cluster = Cluster.connect("127.0.0.1", "admin", "admin123");
    private static final Bucket bucket = cluster.bucket("po");
    private static final Collection collection = bucket.defaultCollection();
    private static final ReactiveCollection reactiveCollection = collection.reactive();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final ObjectWriter poWriter = objectMapper.writerFor(PurchaseOrder.class);
    private static final ObjectReader reader = objectMapper.readerFor(PurchaseOrder.class);
    
    public static void consume() {
        ConnectionFactory cfactory = new ConnectionFactory();
        cfactory.setHost(HOST_NAME);
        cfactory.setPort(PORT);
        cfactory.setUsername(USER_NAME);
        cfactory.setPassword(PASSWORD);
        SenderOptions soptions = new SenderOptions()
                .connectionFactory(cfactory);
        ReceiverOptions roptions = new ReceiverOptions()
                .connectionFactory(cfactory);
        Sender sender = RabbitFlux.createSender(soptions);
        Receiver receiver = RabbitFlux.createReceiver(roptions);
        Map<String, Function<GroupedFlux<String, PurchaseOrder>, Publisher<OutboundMessageResult>>> 
                publisherMap = Map.of(
            "Car", (o) -> getCarPublisher(sender, o),
            "Truck", (o) -> getTruckPublisher(sender, o),
            "Motorcycle", (o) -> getMotorcyclePublisher(sender, o));
        receiver
            .consumeAutoAck(PO_QUEUE_NAME)
            .timeout(Duration.ofSeconds(10))
            .doFinally((s) -> {
                log("Purchace Order Consumer in finally for signal " + s);
                receiver.close();
                sender.close();
                cluster.disconnect();
            })
            .map(d -> readJson(new String(d.getBody())))
            .filter(PurchaseOrder::isValid)
            .doOnNext(po -> log("recevied po " + po))
            .doOnDiscard(PurchaseOrder.class, 
                    po -> log("Discarded invalid PO " + po))
            .flatMap(po -> writePoJson(po).map(j -> reactiveCollection
                    .upsert(po.getId(), j)
                    .map(result -> po))
                .orElse(Mono.just(po)))
            .groupBy(po -> po.getType())
            .flatMap((v) -> publisherMap.get(v.key()).apply(v))
            .subscribe();
    }

    private static void log(String msg) {
        System.out.println(Thread.currentThread().getName() + " " + msg);
    }
    
    private static Optional<String> writePoJson(PurchaseOrder po) {
        try {
            return Optional.of(poWriter.writeValueAsString(po));
        } catch (JsonProcessingException ex) {
            log("unable to serialize po");
            ex.printStackTrace(System.out);
            return Optional.empty();
        }
    }
    
    private static PurchaseOrder readJson(String po) {
        try {
            return reader.readValue(po);
        } catch (JsonProcessingException ex) {
            log("unable to serialize po");
            ex.printStackTrace(System.out);
            return PurchaseOrder.builder().build();
        } catch (IOException ex) {
            log("unable to serialize po");
            ex.printStackTrace(System.out);
            return PurchaseOrder.builder().build();
        }
    }

    private static Flux<OutboundMessageResult> getCarPublisher(Sender sender, 
            GroupedFlux<String, PurchaseOrder> input) {
        log("in get car publisher");
        return sender.sendWithPublishConfirms(input
            .map(po -> new Vehicle.Car(po))
            .doOnNext(car -> log("sending car " + car))
            .map(v -> {
                try {
                    return Optional.of(objectMapper
                            .writerFor(Vehicle.Car.class)
                            .writeValueAsString(v));
                } catch (JsonProcessingException ex) {
                    return Optional.<String>empty();
                }
            })
            .filter(o -> o.isPresent())
            .map(o -> o.get())
            .doOnDiscard(String.class, 
                    s -> log("Discarded invalid Car"))
            .map(s -> new OutboundMessage("", 
                    CAR_QUEUE_NAME, 
                    s.getBytes())));
    }
    
    private static Flux<OutboundMessageResult> getMotorcyclePublisher(Sender sender, 
            GroupedFlux<String, PurchaseOrder> input) {
        log("in get motocycle publisher");
        return sender.sendWithPublishConfirms(input
            .map(po -> new Vehicle.Motorcycle(po))
            .doOnNext(motorcycle -> log("sending motorcycle " + motorcycle))
            .map(v -> {
                try {
                    return Optional.of(objectMapper
                            .writerFor(Vehicle.Motorcycle.class)
                            .writeValueAsString(v));
                } catch (JsonProcessingException ex) {
                    return Optional.<String>empty();
                }
            })
            .filter(o -> o.isPresent())
            .map(o -> o.get())
            .doOnDiscard(String.class, 
                    s -> log("Discarded invalid Motorcycle"))
            .map(s -> new OutboundMessage("", 
                    MOTORCYCLE_QUEUE_NAME, 
                    s.getBytes())));
    }
    
    private static Flux<OutboundMessageResult> getTruckPublisher(Sender sender, 
            GroupedFlux<String, PurchaseOrder> input) {
        log("in get truck publisher");
        return sender.sendWithPublishConfirms(input
            .map(po -> new Vehicle.Truck(po))
            .doOnNext(truck -> log("sending truck " + truck))
            .map(v -> {
                try {
                    return Optional.of(objectMapper
                            .writerFor(Vehicle.Truck.class)
                            .writeValueAsString(v));
                } catch (JsonProcessingException ex) {
                    return Optional.<String>empty();
                }
            })
            .filter(o -> o.isPresent())
            .map(o -> o.get())
            .doOnDiscard(String.class, 
                    s -> log("Discarded invalid Truck"))
            .map(s -> new OutboundMessage("", 
                    TRUCK_QUEUE_NAME, 
                    s.getBytes())));
    }
    
}
