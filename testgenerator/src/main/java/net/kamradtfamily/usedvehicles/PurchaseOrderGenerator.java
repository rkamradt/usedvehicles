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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.rabbitmq.client.ConnectionFactory;
import io.github.rkamradt.possibly.PossiblyFunction;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import net.kamradtfamily.usedvehicles.commonobjects.ContextLogging;
import net.kamradtfamily.usedvehicles.commonobjects.PurchaseOrder;
import reactor.core.publisher.Flux;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.SenderOptions;
import reactor.util.function.Tuples;

/**
 *
 * @author randalkamradt
 */
public class PurchaseOrderGenerator {
    private static final String QUEUE_NAME = "po-queue";
    private static final String HOST_NAME = "localhost";
    private static final int PORT = 5672;
    private static final String USER_NAME = "guest";
    private static final String PASSWORD = "guest";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final ObjectWriter writer = objectMapper.writerFor(Payload.class);
    private static final Random random = new Random();
    
    public static void start() {
        final ConnectionFactory cfactory = new ConnectionFactory();
        cfactory.setHost(HOST_NAME);
        cfactory.setPort(PORT);
        cfactory.setUsername(USER_NAME);
        cfactory.setPassword(PASSWORD);
        final SenderOptions soptions = new SenderOptions()
                .connectionFactory(cfactory);
        final Sender sender = RabbitFlux.createSender(soptions);
        sender.declareQueue(QueueSpecification.queue(QUEUE_NAME));        
        sender.sendWithPublishConfirms(
            Flux.generate((sink) -> sink.next(createRandomPurchaseOrder()))
                .cast(PurchaseOrder.class)
                .map(i -> Tuples.of(ContextLogging.builder()
                    .serviceName("PurchaseOrderGenerator")
                    .eventId(UUID.randomUUID().toString())
                    .build(), i))
                .delayElements(Duration.ofMillis(100))
                .take(Duration.ofMillis(1000))
                .doOnNext((o) -> ContextLogging.log(o.getT1(), "produced: " + o.getT2()))
                .map(t -> t.mapT2(PossiblyFunction.of(po -> 
                        writer.writeValueAsString(new Payload(t.getT1().getEventId(),po)))))
                .map(i -> new OutboundMessage("", 
                        QUEUE_NAME, 
                        i.getT2()
                                .doOnException(e -> ContextLogging.log(i.getT1(), "unable to serialize po"))
                                .getValue()
                                .orElse("")
                                .getBytes()))
                .doFinally((s) -> {
                    ContextLogging.log("PurchaseOrderGenerator in finally for signal " + s); // this is done one, no context
                    sender.close();
                })
        )
        .subscribe();
    }
    
    private static PurchaseOrder createRandomPurchaseOrder() {
        String [] types = { "Car", "Truck", "Motorcycle" };
        return PurchaseOrder.builder()
                .id(Long.toHexString(random.nextLong()))
                .price(new BigDecimal(BigInteger.valueOf(random.nextInt(10000000)),2))
                .time(Instant.now().toString())
                .type(types[random.nextInt(types.length)])
                .build();
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
    
}
