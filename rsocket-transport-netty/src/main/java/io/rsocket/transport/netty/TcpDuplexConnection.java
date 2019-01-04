/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.transport.netty;

import io.netty.buffer.ByteBuf;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.FutureMono;
import reactor.netty.NettyPipeline;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An implementation of {@link DuplexConnection} that connects via TCP.
 */
public final class TcpDuplexConnection implements DuplexConnection {

  private final Connection connection;
  private final Disposable channelClosed;
  private final ChannelReadHandler handler;

  /**
   * Creates a new instance
   *
   * @param connection the {@link Connection} to for managing the server
   */
  public TcpDuplexConnection(Connection connection) {
    this.connection = Objects.requireNonNull(connection, "connection must not be null");
    handler = new ChannelReadHandler(connection.channel());

    connection
        .channel()
        .pipeline()
        .remove(NettyPipeline.ReactiveBridge);

    connection
        .channel().pipeline()
        .addLast(NettyPipeline.ReactiveBridge, handler);

    this.channelClosed =
        FutureMono.from(connection.channel().closeFuture())
            .doFinally(
                s -> {
                  if (!isDisposed()) {
                    dispose();
                  }
                })
            .subscribe();
  }

  @Override
  public void dispose() {
    connection.dispose();
  }

  @Override
  public boolean isDisposed() {
    return connection.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return connection
        .onDispose()
        .doFinally(
            s -> {
              if (!channelClosed.isDisposed()) {
                channelClosed.dispose();
              }
            });
  }

  private AtomicBoolean once = new AtomicBoolean(false);

  @Override
  public Flux<Frame> receive() {
    if (!once.compareAndSet(false, true)) {
      return Flux.error(new IllegalStateException("can only subscribe to receive once"));
    }
    return handler.getProcessor().doFinally(s -> once.set(false));
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frames) {
    return Flux.from(frames)
        .transform(
            frameFlux -> {
              if (frameFlux instanceof Fuseable.QueueSubscription) {
                Fuseable.QueueSubscription<Frame> queueSubscription =
                    (Fuseable.QueueSubscription<Frame>) frameFlux;
                queueSubscription.requestFusion(Fuseable.ASYNC);
                return new SendPublisher<>(
                    queueSubscription,
                    frameFlux,
                    connection.channel(),
                    frame -> frame.content().retain(),
                    ByteBuf::readableBytes);
              } else {
                return new SendPublisher<>(
                    frameFlux,
                    connection.channel(),
                    frame -> frame.content().retain(),
                    ByteBuf::readableBytes);
              }
            })
        .then();
  }
}
