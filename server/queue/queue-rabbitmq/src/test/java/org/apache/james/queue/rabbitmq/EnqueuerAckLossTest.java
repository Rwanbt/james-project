/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 ****************************************************************/

package org.apache.james.queue.rabbitmq;

import static org.apache.james.backends.rabbitmq.Constants.EMPTY_ROUTING_KEY;
import static org.apache.james.queue.api.Mails.defaultMail;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.mail.internet.MimeMessage;

import org.apache.james.backends.rabbitmq.RabbitMQExtension;
import org.apache.james.blob.api.PlainBlobId;
import org.apache.james.blob.api.Store;
import org.apache.james.blob.mail.MimeMessagePartsId;
import org.apache.james.metrics.tests.RecordingMetricFactory;
import org.apache.james.queue.rabbitmq.view.RabbitMQMailQueueConfiguration;
import org.apache.james.queue.rabbitmq.view.api.MailQueueView;
import org.apache.james.queue.rabbitmq.view.cassandra.CassandraMailQueueBrowser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.BindingSpecification;
import reactor.rabbitmq.ExchangeSpecification;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.Sender;

class EnqueuerAckLossTest {
    @RegisterExtension
    static RabbitMQExtension rabbitMQ = RabbitMQExtension.singletonRabbitMQ()
        .isolationPolicy(RabbitMQExtension.IsolationPolicy.WEAK);

    @Test
    void acceptedRabbitMQPublishWithLostConfirmationMustNotDeleteStoredMail() throws Exception {
        MailQueueName name = MailQueueName.fromString("ack-loss-" + UUID.randomUUID());
        Sender actualSender = rabbitMQ.getSender();
        actualSender.declareExchange(ExchangeSpecification.exchange(name.toRabbitExchangeName().asString())
            .durable(true).type("direct")).block();
        actualSender.declareQueue(QueueSpecification.queue(name.toWorkQueueName().asString())
            .durable(true)).block();
        actualSender.bind(BindingSpecification.binding()
            .exchange(name.toRabbitExchangeName().asString())
            .queue(name.toWorkQueueName().asString())
            .routingKey(EMPTY_ROUTING_KEY)).block();

        Store<MimeMessage, MimeMessagePartsId> mimeStore = mock(Store.class);
        PlainBlobId.Factory blobIds = new PlainBlobId.Factory();
        MimeMessagePartsId parts = MimeMessagePartsId.builder()
            .headerBlobId(blobIds.parse("ack-loss-header"))
            .bodyBlobId(blobIds.parse("ack-loss-body"))
            .build();
        when(mimeStore.save(any())).thenReturn(Mono.just(parts));

        MailQueueView<CassandraMailQueueBrowser.CassandraMailQueueItemView> view = mock(MailQueueView.class);
        AtomicBoolean stored = new AtomicBoolean();
        when(view.storeMail(any())).thenAnswer(ignored -> Mono.fromRunnable(() -> stored.set(true)));
        when(view.delete(any())).thenAnswer(ignored -> Mono.fromCallable(() -> {
            stored.set(false);
            return 1L;
        }));

        // Let the real broker ACK the persistent message, then drop the ACK at
        // the caller boundary. A producer cannot distinguish this from failure.
        AtomicBoolean accepted = new AtomicBoolean();
        Sender ackLostSender = mock(Sender.class);
        when(ackLostSender.sendWithPublishConfirms(any())).thenAnswer(invocation ->
            Flux.from(actualSender.sendWithPublishConfirms(invocation.getArgument(0)))
                .concatMap(confirm -> {
                    if (confirm.isAck()) {
                        accepted.set(true);
                    }
                    return Mono.error(new IOException("publish confirmation lost"));
                }));

        Enqueuer enqueuer = new Enqueuer(name, ackLostSender, mimeStore,
            new MailReferenceSerializer(), new RecordingMetricFactory(), view,
            RabbitMQMailQueueConfiguration.builder().mailQueuePublishConfirmEnabled(true).build(),
            Clock.systemUTC());

        assertThatThrownBy(() -> enqueuer.enQueue(defaultMail().build()).block(Duration.ofSeconds(10)))
            .hasMessageContaining("publish confirmation lost");
        assertThat(accepted).hasValue(true);
        assertThat(stored).hasValue(true);
        verify(view, never()).delete(any());

        // The reference is really present in RabbitMQ. In Cassandra-backed
        // deployments a deleted view would instead cause Dequeuer to drop it.
        assertThat(rabbitMQ.getRabbitChannelPool().createReceiver()
            .consumeAutoAck(name.toWorkQueueName().asString())
            .next().block(Duration.ofSeconds(10))).isNotNull();
    }
}
