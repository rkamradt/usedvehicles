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

import com.rabbitmq.client.ConnectionFactory;
import reactor.core.publisher.Hooks;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.SenderOptions;

/**
 *
 * @author randalkamradt
 */
public class Main {
    private static final String PO_QUEUE_NAME = "po-queue";
    private static final String CAR_QUEUE_NAME = "car-queue";
    private static final String TRUCK_QUEUE_NAME = "truck-queue";
    private static final String MOTORCYCLE_QUEUE_NAME = "motorcycle-queue";
    private static final String HOST_NAME = "localhost";
    private static final int PORT = 5672;
    private static final String USER_NAME = "guest";
    private static final String PASSWORD = "guest";
    static final Factory<String, Vehicle, PurchaseOrder> factory = new Factory<>();
    public static void main(String [] args) throws InterruptedException {
        Hooks.onErrorDropped(error -> {
            ContextLogging.log("error dropped " + error);
        });
        ConnectionFactory cfactory = new ConnectionFactory();
        cfactory.setHost(HOST_NAME);
        cfactory.setPort(PORT);
        cfactory.setUsername(USER_NAME);
        cfactory.setPassword(PASSWORD);
        SenderOptions soptions = new SenderOptions()
                .connectionFactory(cfactory);
        try (Sender sender = RabbitFlux.createSender(soptions)) {
            sender.declareQueue(QueueSpecification.queue(PO_QUEUE_NAME));
            sender.declareQueue(QueueSpecification.queue(CAR_QUEUE_NAME));
            sender.declareQueue(QueueSpecification.queue(TRUCK_QUEUE_NAME));
            sender.declareQueue(QueueSpecification.queue(MOTORCYCLE_QUEUE_NAME));
        }
        CarConsumer.consume();
        TruckConsumer.consume();
        MotorcycleConsumer.consume();
        PurchaseOrderGenerator.start();
        PurchaseOrderConsumer.consume();
    }
    
}
