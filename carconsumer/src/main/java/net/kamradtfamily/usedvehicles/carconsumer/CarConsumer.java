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
package net.kamradtfamily.usedvehicles.carconsumer;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCollection;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.rabbitmq.client.ConnectionFactory;
import io.github.rkamradt.possibly.PossiblyFunction;
import net.kamradtfamily.usedvehicles.commonobjects.ContextLogging;
import net.kamradtfamily.usedvehicles.commonobjects.DefaultEnvironmentProperties;
import net.kamradtfamily.usedvehicles.commonobjects.EnvironmentProperties;
import net.kamradtfamily.usedvehicles.commonobjects.Vehicle;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
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
public class CarConsumer {
    private static final EnvironmentProperties env = new DefaultEnvironmentProperties();
    private static final String DATABASE_USER_NAME = 
            env.getEnvironmentProperties("database.user.name").orElseThrow();
    private static final String DATABASE_HOST_NAME = 
            env.getEnvironmentProperties("database.host.name").orElseThrow();
    private static final String DATABASE_PASSWORD  = 
            env.getEnvironmentProperties("database.password").orElseThrow();
    private static final String DATABASE_BUCKET_NAME_PO = 
            env.getEnvironmentProperties("database.bucket.name.po").orElseThrow();
    private static final String DATABASE_BUCKET_NAME_CAR  = 
            env.getEnvironmentProperties("database.bucket.name.car").orElseThrow();
    private static final String QUEUE_HOST_NAME  = 
            env.getEnvironmentProperties("queue.host.name").orElseThrow();
    private static final String QUEUE_PORT = 
            env.getEnvironmentProperties("queue.port").orElseThrow();
    private static final String QUEUE_USER_NAME = 
            env.getEnvironmentProperties("queue.user.name").orElseThrow();
    private static final String QUEUE_PASSWORD = 
            env.getEnvironmentProperties("queue.password").orElseThrow();
    private static final String QUEUE_TOPIC_CAR = 
            env.getEnvironmentProperties("queue.topic.car").orElseThrow();
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    static final ObjectReader carReader = objectMapper.readerFor(Payload.class);
    static final Cluster cluster = Cluster.connect(DATABASE_HOST_NAME, 
            DATABASE_USER_NAME, 
            DATABASE_PASSWORD);
    static final ReactiveCollection poReactiveCollection = 
            cluster.bucket(DATABASE_BUCKET_NAME_PO)
            .defaultCollection()
            .reactive();
    static final ReactiveCollection carReactiveCollection = 
            cluster.bucket(DATABASE_BUCKET_NAME_CAR)
            .defaultCollection()
            .reactive();
    
    public static void consume() {
        ConnectionFactory cfactory = new ConnectionFactory();
        cfactory.setHost(QUEUE_HOST_NAME);
        cfactory.setPort(Integer.valueOf(QUEUE_PORT));
        cfactory.setUsername(QUEUE_USER_NAME);
        cfactory.setPassword(QUEUE_PASSWORD);
        SenderOptions soptions = new SenderOptions()
                .connectionFactory(cfactory);
        Sender sender = RabbitFlux.createSender(soptions);
        ReceiverOptions roptions = new ReceiverOptions()
                .connectionFactory(cfactory);
        Receiver carReceiver = RabbitFlux.createReceiver(roptions);
        carReceiver
            .consumeAutoAck(QUEUE_TOPIC_CAR)
            .onErrorStop()
            .doFinally((s) -> {
                ContextLogging.log("Car consumer in finally for signal " + s);
                carReceiver.close();
                sender.close();
            })
            .doOnNext(d -> ContextLogging.log("received car " + new String(d.getBody())))
            .map(PossiblyFunction.of(d -> carReader.readValue(new String(d.getBody()))))
            .map(p -> p.getValue().orElseGet(() -> new Payload("unknown event id", new Vehicle.Car())))
            .cast(Payload.class)
            .map(c -> Tuples.of(ContextLogging.builder()
                    .eventId(c.eventId)
                    .serviceName("CarConsumer")
                    .build(),new Vehicle.Car(c.car.getPo(),"car lot a")))
            .flatMap(t -> Flux.combineLatest(r -> (Tuple2<ContextLogging,Vehicle.Car>)r[0], 
                    verifyCar(t), 
                    writeCar(t), 
                    wasteTime(t)))   
            .subscribe(t -> ContextLogging.log(t.getT1(), "received car " + t.getT2()));
        
    }
    
    private static Mono<Tuple2<ContextLogging,Vehicle.Car>> wasteTime(Tuple2<ContextLogging,Vehicle.Car> car) {
        return Mono.fromCallable(() -> { 
            Thread.sleep(50);
            return car;
        })
        .subscribeOn(Schedulers.boundedElastic())
        .doOnNext(t -> ContextLogging.log(t.getT1(), "wasting time on car " + t.getT2()));
    }
    
    private static Mono<Tuple2<ContextLogging,Vehicle.Car>> verifyCar(Tuple2<ContextLogging,Vehicle.Car> car) {
        return poReactiveCollection
                    .get(car.getT2().getPo().getId())
                    .doOnNext(c -> ContextLogging.log(car.getT1(), "po for car " + car.getT2() + " confirmed"))
                    .doOnError(t -> ContextLogging.log(car.getT1(), "error verifying car " + car.getT2() + t))
                    .map(j -> car)
                    .single()
                    .onErrorReturn(car);
    }
    
    private static Mono<Tuple2<ContextLogging,Vehicle.Car>> writeCar(Tuple2<ContextLogging,Vehicle.Car> car) {
        return carReactiveCollection
                .upsert(car.getT2().getPo().getId(), car.getT2())
                .doOnNext(c -> ContextLogging.log(car.getT1(), "inserted car " + car.getT2() + " into car database"))
                .doOnError((t) -> ContextLogging.log(car.getT1(), "error inserting car"))
                .map(j -> car)
                .single()
                .onErrorReturn(car);
    }
    
    public static class Payload {
        public Payload() {}
        public Payload(String eventId, Vehicle.Car car) {
            this.eventId = eventId;
            this.car = car;
        }
        public String eventId;
        public Vehicle.Car car;
    }
    
    
}
