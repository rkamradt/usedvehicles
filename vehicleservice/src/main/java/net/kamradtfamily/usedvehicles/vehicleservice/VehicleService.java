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
package net.kamradtfamily.usedvehicles.vehicleservice;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCollection;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.rabbitmq.client.ConnectionFactory;
import express.Express;
import express.utils.Status;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import io.github.rkamradt.possibly.PossiblyFunction;
import net.kamradtfamily.usedvehicles.commonobjects.ContextLogging;
import net.kamradtfamily.usedvehicles.commonobjects.DefaultEnvironmentProperties;
import net.kamradtfamily.usedvehicles.commonobjects.EnvironmentProperties;
import net.kamradtfamily.usedvehicles.commonobjects.PurchaseOrder;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 *
 * @author randalkamradt
 */
public class VehicleService {
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

    private static final Cluster cluster = Cluster.connect(DATABASE_HOST_NAME, 
            DATABASE_USER_NAME, 
            DATABASE_PASSWORD);
    private static final Bucket bucket = cluster.bucket(DATABASE_BUCKET_NAME_PO);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final ObjectWriter payloadWriter = objectMapper.writerFor(Payload.class);
    private static final ConnectionFactory cfactory = new ConnectionFactory();
    static final ReactiveCollection poReactiveCollection =
            cluster.bucket(DATABASE_BUCKET_NAME_PO)
                    .defaultCollection()
                    .reactive();
    public static void main(String [] args) {
        cfactory.setHost(QUEUE_HOST_NAME);
        cfactory.setPort(Integer.parseInt(QUEUE_PORT));
        cfactory.setUsername(QUEUE_USER_NAME);
        cfactory.setPassword(QUEUE_PASSWORD);
        final SenderOptions soptions = new SenderOptions()
                .connectionFactory(cfactory);
        final Sender sender = RabbitFlux.createSender(soptions);
        Express app = new Express();
        app.post("/po", (req, res) -> {
            PurchaseOrderPayload purchaseOrderPayload;
            try {
                purchaseOrderPayload = objectMapper.readValue(req.getBody(),
                        PurchaseOrderPayload.class);
            } catch (IOException e) {
                ContextLogging.log(
                        "could not parse payload " + e.getMessage());
                res.setStatus(Status._400);
                res.send("Unable to parse payload");
                return;
            }
            try {
                if(!List.of("Car", "Truck", "Motorcycle")
                        .contains(purchaseOrderPayload.type)) {
                    ContextLogging.log(
                            "invalid type " + purchaseOrderPayload.type);
                    res.setStatus(Status._400);
                    res.send("type must be one of Car, Truck, or Motorcycle");
                    return;
                }
                String vehicleId = addPayloadToQueue(purchaseOrderPayload, sender)
                    .block(Duration.ofSeconds(10));
                res.setStatus(Status._201);
                res.send(vehicleId);
            } catch (Exception ex) {
                ContextLogging.log("uncaught exception " + ex.getMessage());
                res.sendStatus(Status._500);
            }
        });
        app.get("/po/:poId", (req, res) -> {
            try {
                String id = req.getParam("poId");
                Tuple2<Status, String> payload = getPayloadById(id)
                    .map(PossiblyFunction.of(po -> objectMapper.writeValueAsString(po)))
                    .map(p -> {
                        if(p.exceptional()) {
                            return Tuples.of(Status._500, "Uncaught exception " + p.getException().get());
                        }
                        return Tuples.of(Status._200, p.getValue().get());
                    })
                    .defaultIfEmpty(Tuples.of(Status._404, id + " not found"))
                    .block(Duration.ofSeconds(10));
                res.setStatus(payload.getT1());
                res.send(payload.getT2());
            } catch (Exception ex) {
                ContextLogging.log("uncaught exception " + ex.getMessage());
                ex.printStackTrace(System.out);
                res.sendStatus(Status._500);
            }
        });
        ContextLogging.log("listening on port 80");
        app.listen();    
    }

    private static Mono<String> addPayloadToQueue(
            final PurchaseOrderPayload purchaseOrderPayload,
            final Sender sender) {
        String eventId = UUID.randomUUID().toString();
        String purchaseOrderId = UUID.randomUUID().toString();
        return sender.sendWithPublishConfirms(
           Mono.just(purchaseOrderPayload)
                .doOnNext(po -> ContextLogging.log(
                        "received: " + po))
                .map(p -> new Payload(eventId, PurchaseOrder
                        .builder()
                        .id(purchaseOrderId)
                        .price(p.getPrice())
                        .time(Instant.now().toString())
                        .type(p.getType())
                        .build()))
                .map(i -> Tuples.of(ContextLogging.builder()
                        .serviceName("VehicleService")
                        .eventId(eventId)
                        .build(), i))
                .doOnNext((o) -> ContextLogging.log(o.getT1(),
                        "adding to queue: " + o.getT2()))
                .map(t -> t.mapT2(PossiblyFunction.of(po ->
                        payloadWriter.writeValueAsString(t.getT2()))))
                .map(i -> new OutboundMessage("",
                        QUEUE_TOPIC_PO,
                        i.getT2()
                            .doOnException(e ->
                                    ContextLogging.log(i.getT1(),
                                            e.getMessage()))
                            .getValue()
                            .orElseThrow(() ->
                                    new RuntimeException(
                                            "error sending purchase order"))
                            .getBytes()))

        ) // we could check the return here
        .map(r -> purchaseOrderId) // replace with purchaseOrderId
        .singleOrEmpty();
    }

    public static Mono<PurchaseOrderPayload>
                getPayloadById(final String id) {
        final ContextLogging context = ContextLogging.builder()
                .serviceName("VehicleService")
                .eventId(UUID.randomUUID().toString())
                .build();
        return poReactiveCollection
                .get(id)
                .doOnNext(c -> ContextLogging.log(context,
                        "po " + id + " found " + c.toString()))
                .doOnError(e -> ContextLogging.log(context,
                        "error finding po " + id + " " + e))
                .map(r -> r.contentAs(String.class))
                .map(PossiblyFunction.of(ps -> objectMapper.readValue(ps, Payload.class)))
                .map(p -> p.map(po -> PurchaseOrderPayload.builder()
                        .price(po.po.getPrice())
                        .type(po.po.getType())
                        .build()))
                .map(p -> p.getValue())
                .onErrorReturn(DocumentNotFoundException.class, Optional.empty())
                .flatMap(p -> p.isEmpty() ? Mono.empty() : Mono.just(p.get()));
    }

    public static class Payload {
        public Payload() {} // need for Couchbase serialization
        public Payload(String eventId, PurchaseOrder po) {
            this.eventId = eventId;
            this.po = po;
        }
        public String eventId;
        public PurchaseOrder po;
    }

}
