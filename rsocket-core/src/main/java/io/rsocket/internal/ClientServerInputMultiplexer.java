/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.internal;

import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.framing.FrameType;
import io.rsocket.plugins.DuplexConnectionInterceptor.Type;
import io.rsocket.plugins.PluginRegistry;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.DirectProcessor;
import reactor.util.concurrent.Queues;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * {@link DuplexConnection#receive()} is a single stream on which the following type of frames
 * arrive:
 *
 * <ul>
 * <li>Frames for streams initiated by the initiator of the connection (client).
 * <li>Frames for streams initiated by the acceptor of the connection (server).
 * </ul>
 *
 * <p>The only way to differentiate these two frames is determining whether the stream Id is odd or
 * even. Even IDs are for the streams initiated by server and odds are for streams initiated by the
 * client.
 */
public class ClientServerInputMultiplexer implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger("io.rsocket.FrameLogger");

  private final DuplexConnection streamZeroConnection;
  private final DuplexConnection serverConnection;
  private final DuplexConnection clientConnection;
  private final DuplexConnection source;

  public ClientServerInputMultiplexer(DuplexConnection source, PluginRegistry plugins) {
    this.source = source;

    DirectProcessor<Frame> streamZero = DirectProcessor.create();
    DirectProcessor<Frame> server = DirectProcessor.create();
    DirectProcessor<Frame> client = DirectProcessor.create();

    source = plugins.applyConnection(Type.SOURCE, source);
    streamZeroConnection =
        plugins.applyConnection(Type.STREAM_ZERO, new InternalDuplexConnection(source, streamZero));
    serverConnection =
        plugins.applyConnection(Type.SERVER, new InternalDuplexConnection(source, server));
    clientConnection =
        plugins.applyConnection(Type.CLIENT, new InternalDuplexConnection(source, client));

    source
        .receive()
        .subscribe(new InnerSubscriber(streamZero, server, client));
  }

  private volatile long request = 256;

  private class InnerSubscriber extends AtomicBoolean implements Subscriber<Frame> {
    private volatile Subscription s;

    final DirectProcessor<Frame> streamZero;
    final DirectProcessor<Frame> server;
    final DirectProcessor<Frame> client;

    public InnerSubscriber(DirectProcessor<Frame> streamZero, DirectProcessor<Frame> server, DirectProcessor<Frame> client) {
      this.streamZero = streamZero;
      this.server = server;
      this.client = client;
    }

    @Override
    public void onSubscribe(Subscription s) {
      this.s = s;
      s.request(256);
    }

    @Override
    public void onNext(Frame frame) {
      if (get()) {
        frame.release();
        return;
      }

      int streamId = frame.getStreamId();
      if (streamId == 0) {
        if (frame.getType() == FrameType.SETUP) {
          streamZero.onNext(frame);
        } else {
          client.onNext(frame);
        }
      } else if ((streamId & 0b1) == 0) {
        server.onNext(frame);
      } else {
        client.onNext(frame);
      }

      long r = 0;
      synchronized (ClientServerInputMultiplexer.this) {
        request--;
        if (request <= 64) {
          r = 256 - request;
          request = 256;
        }
      }

      if (r > 0) {
       s.request(r);
      }
    }

    @Override
    public void onError(Throwable t) {
      set(true);
      LOGGER.error("Error receiving frame:", t);
      dispose();
    }

    @Override
    public void onComplete() {
      set(true);
    }
  }

  public DuplexConnection asServerConnection() {
    return serverConnection;
  }

  public DuplexConnection asClientConnection() {
    return clientConnection;
  }

  public DuplexConnection asStreamZeroConnection() {
    return streamZeroConnection;
  }

  @Override
  public void dispose() {
    source.dispose();
  }

  @Override
  public boolean isDisposed() {
    return source.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return source.onClose();
  }

  private static class InternalDuplexConnection implements DuplexConnection {
    private final DuplexConnection source;
    private final FluxProcessor<Frame, Frame> processor;
    private final boolean debugEnabled;

    public InternalDuplexConnection(DuplexConnection source, FluxProcessor<Frame, Frame> processor) {
      this.source = source;
      this.processor = processor;
      this.debugEnabled = LOGGER.isDebugEnabled();
    }

    @Override
    public Mono<Void> send(Publisher<Frame> frame) {
      if (debugEnabled) {
        frame = Flux.from(frame).doOnNext(f -> LOGGER.debug("sending -> " + f.toString()));
      }

      return source.send(frame);
    }

    @Override
    public Mono<Void> sendOne(Frame frame) {
      if (debugEnabled) {
        LOGGER.debug("sending -> " + frame.toString());
      }

      return source.sendOne(frame);
    }

    @Override
    public Flux<Frame> receive() {
      return processor.map(
          f -> {
            if (debugEnabled) {
              LOGGER.debug("receiving -> " + f.toString());
            }

            return f;
          });
    }

    @Override
    public void dispose() {
      source.dispose();
    }

    @Override
    public boolean isDisposed() {
      return source.isDisposed();
    }

    @Override
    public Mono<Void> onClose() {
      return source.onClose();
    }

    @Override
    public double availability() {
      return source.availability();
    }
  }
}
