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
package net.kamradtfamily.usedvehicles.purchaseorderconsumer;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.rabbitmq.client.ConnectionFactory;
import io.github.rkamradt.possibly.PossiblyFunction;
import java.util.Map;
import java.util.function.Function;
import net.kamradtfamily.usedvehicles.commonobjects.ContextLogging;
import net.kamradtfamily.usedvehicles.commonobjects.DefaultEnvironmentProperties;
import net.kamradtfamily.usedvehicles.commonobjects.EnvironmentProperties;
import net.kamradtfamily.usedvehicles.commonobjects.PurchaseOrder;
import net.kamradtfamily.usedvehicles.commonobjects.Vehicle;
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
    private static final EnvironmentProperties env = new DefaultEnvironmentProperties();
    private static final String DATABASE_USER_NAME = 
            env.getEnvironmentProperties("database.user.name").orElseThrow();
    private static final String DATABASE_HOST_NAME = 
            env.getEnvironmentProperties("database.host.name").orElseThrow();
    private static final String DATABASE_PASSWORD  = 
            env.getEnvironmentProperties("database.password").orElseThrow();
    private static final String DATABASE_BUCKET_NAME_PO = 
            env.getEnvironmentProperties("database.bucket.name.po").orElseThrow();
    private static final String QUEUE_HOST_NAME  = 
            env.getEnvironmentProperties("queue.host.name").orElseThrow();
    private static final String QUEUE_PORT = 
            env.getEnvironmentProperties("queue.port").orElseThrow();
    private static final String QUEUE_USER_NAME = 
            env.getEnvironmentProperties("queue.user.name").orElseThrow();
    private static final String QUEUE_PASSWORD = 
            env.getEnvironmentProperties("queue.password").orElseThrow();
    private static final String QUEUE_TOPIC_PO = 
            env.getEnvironmentProperties("queue.topic.po").orElseThrow();
    private static final String QUEUE_TOPIC_CAR = 
            env.getEnvironmentProperties("queue.topic.car").orElseThrow();
    private static final String QUEUE_TOPIC_TRUCK = 
            env.getEnvironmentProperties("queue.topic.truck").orElseThrow();
    private static final String QUEUE_TOPIC_MOTORCYCLE = 
            env.getEnvironmentProperties("queue.topic.motorcycle").orElseThrow();
    
    private static final Cluster cluster = Cluster.connect(DATABASE_HOST_NAME, 
            DATABASE_USER_NAME, 
            DATABASE_PASSWORD);
    private static final Bucket bucket = cluster.bucket(DATABASE_BUCKET_NAME_PO);
    private static final Collection collection = bucket.defaultCollection();
    private static final ReactiveCollection reactiveCollection = collection.reactive();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final ObjectWriter poWriter = objectMapper.writerFor(Payload.class);
    private static final ObjectWriter carWriter = objectMapper.writerFor(CarPayload.class);
    private static final ObjectWriter motorcycleWriter = objectMapper.writerFor(MotorcyclePayload.class);
    private static final ObjectWriter truckWriter = objectMapper.writerFor(TruckPayload.class);
    private static final ObjectReader reader = objectMapper.readerFor(Payload.class);
    
    public static void consume() {
        ConnectionFactory cfactory = new ConnectionFactory();
        cfactory.setHost(QUEUE_HOST_NAME);
        cfactory.setPort(Integer.parseInt(QUEUE_PORT));
        cfactory.setUsername(QUEUE_USER_NAME);
        cfactory.setPassword(QUEUE_PASSWORD);
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
            .consumeAutoAck(QUEUE_TOPIC_PO)
            .doFinally((s) -> {
                ContextLogging.log("Purchace Order Consumer in finally for signal " + s);
                receiver.close();
                sender.close();
                cluster.disconnect();
            })
            .map(PossiblyFunction.of(p -> reader.readValue((new String(p.getBody())))))
            .map(p -> p.exceptional() 
                    ? new Payload("unknown event id", PurchaseOrder.builder().build())
                    : p.getValue().get())
            .cast(Payload.class)
            .map(p -> Tuples.of(ContextLogging.builder()
                    .serviceName("PurchaseOrderConsumer")
                    .eventId(p.eventId)
                    .build(), p.po))
            .filter(t -> PurchaseOrder.isValid(t.getT2()))
            .doOnNext(t -> ContextLogging.log(t.getT1(), "recevied po " + t.getT2()))
            .doOnDiscard(Tuple2.class, 
                    t -> ContextLogging.log((ContextLogging)t.getT1(), "Discarded invalid PO " + t.getT2()))
            .flatMap(t -> PossiblyFunction.of(s -> poWriter.writeValueAsString(new Payload(t.getT1().getEventId(),t.getT2()))).apply(t)
                .getValue()
                .map(j -> reactiveCollection
                    .upsert(t.getT2().getId(), j)
                    .doOnNext(r -> ContextLogging.log(t.getT1(), "upserted po"))
                    .doOnError(r -> ContextLogging.log(t.getT1(), "error upserting po"))
                    .map(result -> t))
                .orElse(Mono.just(t)))
            .groupBy(t -> t.getT2().getType())
            .flatMap(v -> publisherMap.get(v.key()).apply(v))
            .subscribe();
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
            .map(PossiblyFunction.of(t -> carWriter.writeValueAsString(new CarPayload(t.getT1().getEventId(), t.getT2()))))
            .doOnNext(p -> p.doOnException(e -> ContextLogging.log("exception writing car to json" + e)))
            .map(p -> p.getValue())
            .filter(o -> o.isPresent())
            .map(o -> o.get())
            .doOnDiscard(String.class, 
                    s -> ContextLogging.log("Discarded invalid Car"))
            .map(s -> new OutboundMessage("", 
                    QUEUE_TOPIC_CAR, 
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
            .map(PossiblyFunction.of(t -> motorcycleWriter.writeValueAsString(new MotorcyclePayload(t.getT1().getEventId(), t.getT2()))))
            .map(p -> p.getValue())
            .filter(o -> o.isPresent())
            .map(o -> o.get())
            .doOnDiscard(String.class, 
                    s -> ContextLogging.log("Discarded invalid Motorcycle"))
            .map(s -> new OutboundMessage("", 
                    QUEUE_TOPIC_MOTORCYCLE, 
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
            .map(PossiblyFunction.of(t -> truckWriter.writeValueAsString(new TruckPayload(t.getT1().getEventId(), t.getT2()))))
            .map(p -> p.getValue())
            .filter(o -> o.isPresent())
            .map(o -> o.get())
            .doOnDiscard(String.class, 
                    s -> ContextLogging.log("Discarded invalid Truck"))
            .map(s -> new OutboundMessage("", 
                    QUEUE_TOPIC_TRUCK, 
                    s.getBytes())));
    }
    
}
