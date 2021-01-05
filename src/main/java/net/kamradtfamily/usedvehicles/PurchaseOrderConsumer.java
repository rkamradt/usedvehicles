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
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
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
    static final Factory<String, Vehicle, PurchaseOrder> factory = new Factory<>();
    static final Cluster cluster = Cluster.connect("127.0.0.1", "admin", "admin123");
    static final Bucket bucket = cluster.bucket("po");
    static final Collection collection = bucket.defaultCollection();
    static final ReactiveCollection reactiveCollection = collection.reactive();
    static final ObjectMapper objectMapper = new ObjectMapper();
    static final ObjectWriter poWriter = objectMapper.writerFor(PurchaseOrder.class);
    static final ObjectWriter carWriter = objectMapper.writerFor(Vehicle.Car.class);
    static final ObjectWriter truckWriter = objectMapper.writerFor(Vehicle.Truck.class);
    static final ObjectWriter motorcycleWriter = objectMapper.writerFor(Vehicle.Motorcycle.class);
    static final ObjectReader reader = objectMapper.readerFor(PurchaseOrder.class);
    static final Map<String, String> outQueue = Map.of(
            "Car", CAR_QUEUE_NAME,
            "Truck", TRUCK_QUEUE_NAME,
            "Motorcycle", MOTORCYCLE_QUEUE_NAME);
    static final Map<String, Function<Vehicle, Optional<String>>> vehicleWriter = Map.of(
            "Car", (s) -> writeCarJson((Vehicle.Car)s),
            "Truck", (s) -> writeTruckJson((Vehicle.Truck)s),
            "Motorcycle", (s) -> writeMotorcycleJson((Vehicle.Motorcycle)s));
    
    public static void consume() {
        factory.add("Car", (po) -> new Vehicle.Car(po));
        factory.add("Truck", (po) -> new Vehicle.Truck(po));
        factory.add("Motorcycle", (po) -> new Vehicle.Motorcycle(po));
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
        sender.sendWithPublishConfirms(receiver
            .consumeAutoAck(PO_QUEUE_NAME)
            .map(d -> readJson(new String(d.getBody())))
            .filter(PurchaseOrder::isValid)
            .doOnNext(po -> log("recevied po " + po))
            .doOnDiscard(PurchaseOrder.class, 
                    po -> log("Discarded invalid PO " + po))
            .flatMap(po -> writePoJson(po).map(j -> reactiveCollection
                    .upsert(po.getId(), j)
                    .map(result -> po))
                .orElse(Mono.just(po)))
            .map(gf -> factory.build(gf.getType(), gf))
            .timeout(Duration.ofSeconds(10))
            .doFinally((s) -> {
                log("Consumer in finally for signal " + s);
                receiver.close();
                sender.close();
                cluster.disconnect();
            })
            .map(v -> new OutboundMessage("", 
                    outQueue.get(v.getPo().getType()), 
                    vehicleWriter
                            .get(v.getPo().getType())
                            .apply(v)
                            .orElse("")
                            .getBytes()))
        )
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
    
    static Optional<String> writeCarJson(Vehicle.Car car) {
        try {
            return Optional.of(carWriter.writeValueAsString(car));
        } catch (JsonProcessingException ex) {
            log("unable to serialize car");
            ex.printStackTrace(System.out);
            return Optional.empty();
        }
    }
    
    static Optional<String> writeTruckJson(Vehicle.Truck truck) {
        try {
            return Optional.of(truckWriter.writeValueAsString(truck));
        } catch (JsonProcessingException ex) {
            log("unable to serialize truck");
            ex.printStackTrace(System.out);
            return Optional.empty();
        }
    }
    
    static Optional<String> writeMotorcycleJson(Vehicle.Motorcycle motorcycle) {
        try {
            return Optional.of(motorcycleWriter.writeValueAsString(motorcycle));
        } catch (JsonProcessingException ex) {
            log("unable to serialize motorcycle");
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
    
    
}
