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

package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.*;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.LimitableRequestPublisher;
import io.rsocket.internal.UnboundedProcessor;
import java.util.function.Consumer;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.UnicastProcessor;

/** Server side RSocket. Receives {@link ByteBuf}s from a {@link RSocketClient} */
class RSocketServer implements RSocket {

  private final DuplexConnection connection;
  private final RSocket requestHandler;
  private final PayloadDecoder payloadDecoder;
  private final Consumer<Throwable> errorConsumer;

  private final SubscriptionHolder sendingSubscriptions;
  private final ChannelProcessorHolder channelProcessors;

  private final UnboundedProcessor<ByteBuf> sendProcessor;
  private final ByteBufAllocator allocator;
  private KeepAliveHandler keepAliveHandler;

  /*client responder*/
  RSocketServer(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer) {
    this(allocator, connection, requestHandler, payloadDecoder, errorConsumer, 0, 0);
  }

  /*server responder*/
  RSocketServer(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer,
      long tickPeriod,
      long ackTimeout) {
    this.allocator = allocator;
    this.connection = connection;
    this.requestHandler = requestHandler;
    this.payloadDecoder = payloadDecoder;
    this.errorConsumer = errorConsumer;
    this.sendingSubscriptions = new SubscriptionHolder();
    this.channelProcessors = new ChannelProcessorHolder();

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    // connections
    this.sendProcessor = new UnboundedProcessor<>();

    connection
        .send(sendProcessor)
        .doFinally(this::handleSendProcessorCancel)
        .subscribe(null, this::handleSendProcessorError);

    Disposable receiveDisposable = connection.receive().subscribe(this::handleFrame, errorConsumer);

    this.connection
        .onClose()
        .doFinally(
            s -> {
              cleanup();
              receiveDisposable.dispose();
            })
        .subscribe(null, errorConsumer);

    if (tickPeriod != 0) {
      keepAliveHandler =
          KeepAliveHandler.ofServer(new KeepAliveHandler.KeepAlive(tickPeriod, ackTimeout));

      keepAliveHandler
          .timeout()
          .subscribe(
              keepAlive -> {
                String message =
                    String.format("No keep-alive acks for %d ms", keepAlive.getTimeoutMillis());
                errorConsumer.accept(new ConnectionErrorException(message));
                connection.dispose();
              });
      keepAliveHandler.send().subscribe(sendProcessor::onNext);
    } else {
      keepAliveHandler = null;
    }
  }

  private void handleSendProcessorError(Throwable t) {
    sendingSubscriptions.cancel(errorConsumer);

    channelProcessors.cancel(errorConsumer, t);
  }

  private void handleSendProcessorCancel(SignalType t) {
    if (SignalType.ON_ERROR == t) {
      return;
    }

    sendingSubscriptions.cancel(errorConsumer);
    channelProcessors.complete(errorConsumer);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    try {
      return requestHandler.fireAndForget(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      return requestHandler.requestResponse(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    try {
      return requestHandler.requestStream(payload);
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    try {
      return requestHandler.requestChannel(payloads);
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    try {
      return requestHandler.metadataPush(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
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
    return connection.onClose();
  }

  private void cleanup() {
    if (keepAliveHandler != null) {
      keepAliveHandler.dispose();
    }
    cleanUpSendingSubscriptions();
    cleanUpChannelProcessors();

    requestHandler.dispose();
    sendProcessor.dispose();
  }

  private synchronized void cleanUpSendingSubscriptions() {
    sendingSubscriptions.cancel(errorConsumer);
    sendingSubscriptions.clear();
  }

  private synchronized void cleanUpChannelProcessors() {
    channelProcessors.complete(errorConsumer);
    channelProcessors.clear();
  }

  private void handleFrame(ByteBuf frame) {
    try {
      int streamId = FrameHeaderFlyweight.streamId(frame);
      Subscriber<Payload> receiver;
      FrameType frameType = FrameHeaderFlyweight.frameType(frame);
      switch (frameType) {
        case REQUEST_FNF:
          handleFireAndForget(streamId, fireAndForget(payloadDecoder.apply(frame)));
          break;
        case REQUEST_RESPONSE:
          handleRequestResponse(streamId, requestResponse(payloadDecoder.apply(frame)));
          break;
        case CANCEL:
          handleCancelFrame(streamId);
          break;
        case KEEPALIVE:
          handleKeepAliveFrame(frame);
          break;
        case REQUEST_N:
          handleRequestN(streamId, frame);
          break;
        case REQUEST_STREAM:
          handleStream(
              streamId,
              requestStream(payloadDecoder.apply(frame)),
              RequestStreamFrameFlyweight.initialRequestN(frame));
          break;
        case REQUEST_CHANNEL:
          handleChannel(
              streamId,
              payloadDecoder.apply(frame),
              RequestChannelFrameFlyweight.initialRequestN(frame));
          break;
        case METADATA_PUSH:
          metadataPush(payloadDecoder.apply(frame));
          break;
        case PAYLOAD:
          // TODO: Hook in receiving socket.
          break;
        case LEASE:
          // Lease must not be received here as this is the server end of the socket which sends
          // leases.
          break;
        case NEXT:
          receiver = channelProcessors.get(streamId);
          if (receiver != null) {
            receiver.onNext(payloadDecoder.apply(frame));
          }
          break;
        case COMPLETE:
          receiver = channelProcessors.get(streamId);
          if (receiver != null) {
            receiver.onComplete();
          }
          break;
        case ERROR:
          receiver = channelProcessors.get(streamId);
          if (receiver != null) {
            receiver.onError(new ApplicationErrorException(ErrorFrameFlyweight.dataUtf8(frame)));
          }
          break;
        case NEXT_COMPLETE:
          receiver = channelProcessors.get(streamId);
          if (receiver != null) {
            receiver.onNext(payloadDecoder.apply(frame));
            receiver.onComplete();
          }
          break;
        case SETUP:
          handleError(streamId, new IllegalStateException("Setup frame received post setup."));
          break;
        default:
          handleError(
              streamId,
              new IllegalStateException("ServerRSocket: Unexpected frame type: " + frameType));
          break;
      }
    } finally {
      ReferenceCountUtil.safeRelease(frame);
    }
  }

  private void handleFireAndForget(int streamId, Mono<Void> result) {
    result
        .doOnSubscribe(subscription -> sendingSubscriptions.put(streamId, subscription))
        .doFinally(signalType -> sendingSubscriptions.remove(streamId))
        .subscribe(null, errorConsumer);
  }

  private void handleRequestResponse(int streamId, Mono<Payload> response) {
    response
        .doOnSubscribe(subscription -> sendingSubscriptions.put(streamId, subscription))
        .map(payload -> PayloadFrameFlyweight.encodeNextComplete(allocator, streamId, payload))
        .switchIfEmpty(
            Mono.fromCallable(() -> PayloadFrameFlyweight.encodeComplete(allocator, streamId)))
        .doFinally(signalType -> sendingSubscriptions.remove(streamId))
        .subscribe(t1 -> sendProcessor.onNext(t1), t -> handleError(streamId, t));
  }

  private void handleStream(int streamId, Flux<Payload> response, int initialRequestN) {
    response
        .transform(
            frameFlux -> {
              LimitableRequestPublisher<Payload> payloads =
                  LimitableRequestPublisher.wrap(frameFlux);
              sendingSubscriptions.put(streamId, payloads);
              payloads.increaseRequestLimit(initialRequestN);
              return payloads;
            })
        .doFinally(signalType -> sendingSubscriptions.remove(streamId))
        .subscribe(
            payload ->
                sendProcessor.onNext(
                    PayloadFrameFlyweight.encodeNext(allocator, streamId, payload)),
            t -> handleError(streamId, t),
            () -> sendProcessor.onNext(PayloadFrameFlyweight.encodeComplete(allocator, streamId)));
  }

  private void handleChannel(int streamId, Payload payload, int initialRequestN) {
    UnicastProcessor<Payload> frames = UnicastProcessor.create();
    channelProcessors.put(streamId, frames);

    Flux<Payload> payloads =
        frames
            .doOnCancel(
                () -> sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId)))
            .doOnError(t -> handleError(streamId, t))
            .doOnRequest(
                l -> sendProcessor.onNext(RequestNFrameFlyweight.encode(allocator, streamId, l)))
            .doFinally(signalType -> channelProcessors.remove(streamId));

    // not chained, as the payload should be enqueued in the Unicast processor before this method
    // returns
    // and any later payload can be processed
    frames.onNext(payload);

    handleStream(streamId, requestChannel(payloads), initialRequestN);
  }

  private void handleKeepAliveFrame(ByteBuf frame) {
    if (keepAliveHandler != null) {
      keepAliveHandler.receive(frame);
    }
  }

  private void handleCancelFrame(int streamId) {
    Subscription subscription = sendingSubscriptions.remove(streamId);
    if (subscription != null) {
      subscription.cancel();
    }
  }

  private void handleError(int streamId, Throwable t) {
    errorConsumer.accept(t);
    sendProcessor.onNext(ErrorFrameFlyweight.encode(allocator, streamId, t));
  }

  private void handleRequestN(int streamId, ByteBuf frame) {
    final Subscription subscription = sendingSubscriptions.get(streamId);
    if (subscription != null) {
      int n = RequestNFrameFlyweight.requestN(frame);
      subscription.request(n >= Integer.MAX_VALUE ? Long.MAX_VALUE : n);
    }
  }

  private static class SubscriptionHolder {
    private final IntObjectMap<Subscription> sendingSubscriptions = new IntObjectHashMap<>();

    private synchronized Subscription get(int i) {
      return sendingSubscriptions.get(i);
    }

    private synchronized void put(int i, Subscription subscription) {
      sendingSubscriptions.put(i, subscription);
    }

    private synchronized Subscription remove(int i) {
      return sendingSubscriptions.remove(i);
    }

    private synchronized void cancel(Consumer<Throwable> errorConsumer) {
      sendingSubscriptions
          .values()
          .forEach(
              (subscription) -> {
                try {
                  subscription.cancel();
                } catch (Throwable t) {
                  errorConsumer.accept(t);
                }
              });
    }

    private synchronized void clear() {
      sendingSubscriptions.clear();
    }
  }

  private static class ChannelProcessorHolder {
    IntObjectMap<Processor<Payload, Payload>> channelProcessors = new IntObjectHashMap<>();

    private synchronized Processor<Payload, Payload> get(int i) {
      return channelProcessors.get(i);
    }

    private synchronized void put(int i, Processor<Payload, Payload> subscription) {
      channelProcessors.put(i, subscription);
    }

    private synchronized Processor<Payload, Payload> remove(int i) {
      return channelProcessors.remove(i);
    }

    private synchronized void cancel(Consumer<Throwable> errorConsumer, Throwable error) {
      channelProcessors
          .values()
          .forEach(
              (subscription) -> {
                try {
                  subscription.onError(error);
                } catch (Throwable t) {
                  errorConsumer.accept(t);
                }
              });
    }

    private synchronized void complete(Consumer<Throwable> errorConsumer) {
      channelProcessors
          .values()
          .forEach(
              (subscription) -> {
                try {
                  subscription.onComplete();
                } catch (Throwable t) {
                  errorConsumer.accept(t);
                }
              });
    }

    private synchronized void clear() {
      channelProcessors.clear();
    }
  }
}
