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
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

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
    private static final ObjectWriter poWriter = objectMapper.writerFor(Payload.class);
    private static final ObjectReader reader = objectMapper.readerFor(Payload.class);
    
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
        Map<String, Function<GroupedFlux<String, Tuple2<ContextLogging, PurchaseOrder>>, 
                Publisher<OutboundMessageResult>>> 
                publisherMap = Map.of(
            "Car", (o) -> getCarPublisher(sender, o),
            "Truck", (o) -> getTruckPublisher(sender, o),
            "Motorcycle", (o) -> getMotorcyclePublisher(sender, o));
        receiver
            .consumeAutoAck(PO_QUEUE_NAME)
            .timeout(Duration.ofSeconds(10))
            .doFinally((s) -> {
                ContextLogging.log("Purchace Order Consumer in finally for signal " + s);
                receiver.close();
                sender.close();
                cluster.disconnect();
            })
            .map(p -> readJson(new String(p.getBody())))
            .map(p -> Tuples.of(ContextLogging.builder()
                    .serviceName("PurchaseOrderConsumer")
                    .eventId(p.eventId)
                    .build(), p.po))
            .filter(t -> t.getT2().isValid())
            .doOnNext(t -> ContextLogging.log(t.getT1(), "recevied po " + t.getT2()))
            .doOnDiscard(Tuple2.class, 
                    t -> ContextLogging.log((ContextLogging)t.getT1(), "Discarded invalid PO " + t.getT2()))
            .flatMap(t -> writePoJson(t.getT1(),t.getT2())
                .map(j -> reactiveCollection
                    .upsert(t.getT2().getId(), j)
                    .map(result -> t))
                .orElse(Mono.just(t)))
            .groupBy(t -> t.getT2().getType())
            .flatMap(v -> publisherMap.get(v.key()).apply(v))
            .subscribe();
    }

    private static Optional<String> writePoJson(ContextLogging context, PurchaseOrder po) {
        try {
            Payload payload = new Payload();
            payload.eventId = context.getEventId();
            payload.po = po;
            return Optional.of(poWriter.writeValueAsString(payload));
        } catch (JsonProcessingException ex) {
            ContextLogging.log(context, "unable to serialize po");
            ex.printStackTrace(System.out);
            return Optional.empty();
        }
    }
    
    public static class Payload {
        public Payload() {}
        public Payload(String eventId, PurchaseOrder po) {
            this.eventId = eventId;
            this.po = po;
        }
        public String eventId;
        public PurchaseOrder po;
    }
    
    private static Payload readJson(String po) {
        try {
            return reader.readValue(po);
        } catch (JsonProcessingException ex) {
            ContextLogging.log("unable to serialize po");
            ex.printStackTrace(System.out);
            Payload empty = new Payload();
            empty.eventId = "no event id";
            empty.po = PurchaseOrder.builder().build();
            return empty;
        } catch (IOException ex) {
            ContextLogging.log("unable to serialize po");
            ex.printStackTrace(System.out);
            Payload empty = new Payload();
            empty.eventId = "no event id";
            empty.po = PurchaseOrder.builder().build();
            return empty;
        }
    }

    public static class CarPayload {
        public CarPayload() {}
        public CarPayload(String eventId, Vehicle.Car car) {
            this.eventId = eventId;
            this.car = car;
        }
        public String eventId;
        public Vehicle.Car car;
    }
    
    private static Flux<OutboundMessageResult> getCarPublisher(Sender sender, 
            GroupedFlux<String, Tuple2<ContextLogging, PurchaseOrder>> input) {
        return sender.sendWithPublishConfirms(input
            .map(t -> Tuples.of(t.getT1(), new Vehicle.Car(t.getT2())))
            .doOnNext(t -> ContextLogging.log(t.getT1(), "sending car " + t.getT2()))
            .map(t -> {
                try {
                    return Optional.of(objectMapper
                            .writerFor(CarPayload.class)
                            .writeValueAsString(new CarPayload(t.getT1().getEventId(), t.getT2())));
                } catch (JsonProcessingException ex) {
                    return Optional.<String>empty();
                }
            })
            .filter(o -> o.isPresent())
            .map(o -> o.get())
            .doOnDiscard(String.class, 
                    s -> ContextLogging.log("Discarded invalid Car"))
            .map(s -> new OutboundMessage("", 
                    CAR_QUEUE_NAME, 
                    s.getBytes())));
    }
    
    public static class MotorcyclePayload {
        public MotorcyclePayload() {}
        public MotorcyclePayload(String eventId, Vehicle.Motorcycle motorcycle) {
            this.eventId = eventId;
            this.motorcycle = motorcycle;
        }
        public String eventId;
        public Vehicle.Motorcycle motorcycle;
    }
    
    private static Flux<OutboundMessageResult> getMotorcyclePublisher(Sender sender, 
            GroupedFlux<String, Tuple2<ContextLogging, PurchaseOrder>> input) {
        return sender.sendWithPublishConfirms(input
            .map(t -> Tuples.of(t.getT1(), new Vehicle.Motorcycle(t.getT2())))
            .doOnNext(t -> ContextLogging.log(t.getT1(), "sending motorcycle " + t.getT2()))
            .map(t -> {
                try {
                    return Optional.of(objectMapper
                            .writerFor(MotorcyclePayload.class)
                            .writeValueAsString(new MotorcyclePayload(t.getT1().getEventId(), t.getT2())));
                } catch (JsonProcessingException ex) {
                    return Optional.<String>empty();
                }
            })
            .filter(o -> o.isPresent())
            .map(o -> o.get())
            .doOnDiscard(String.class, 
                    s -> ContextLogging.log("Discarded invalid Motorcycle"))
            .map(s -> new OutboundMessage("", 
                    MOTORCYCLE_QUEUE_NAME, 
                    s.getBytes())));
    }
    
    public static class TruckPayload {
        public TruckPayload() {}
        public TruckPayload(String eventId, Vehicle.Truck truck) {
            this.eventId = eventId;
            this.truck = truck;
        }
        public String eventId;
        public Vehicle.Truck truck;
    }
    
    private static Flux<OutboundMessageResult> getTruckPublisher(Sender sender, 
            GroupedFlux<String, Tuple2<ContextLogging, PurchaseOrder>> input) {
        return sender.sendWithPublishConfirms(input
            .map(t -> Tuples.of(t.getT1(), new Vehicle.Truck(t.getT2())))
            .doOnNext(t -> ContextLogging.log(t.getT1(), "sending truck " + t.getT2()))
            .map(t -> {
                try {
                    return Optional.of(objectMapper
                            .writerFor(TruckPayload.class)
                            .writeValueAsString(new TruckPayload(t.getT1().getEventId(), t.getT2())));
                } catch (JsonProcessingException ex) {
                    return Optional.<String>empty();
                }
            })
            .filter(o -> o.isPresent())
            .map(o -> o.get())
            .doOnDiscard(String.class, 
                    s -> ContextLogging.log("Discarded invalid Truck"))
            .map(s -> new OutboundMessage("", 
                    TRUCK_QUEUE_NAME, 
                    s.getBytes())));
    }
    
}
