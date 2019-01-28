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

package io.rsocket.micrometer;

import static reactor.core.publisher.SignalType.CANCEL;
import static reactor.core.publisher.SignalType.ON_COMPLETE;
import static reactor.core.publisher.SignalType.ON_ERROR;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Sample;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

/**
 * An implementation of {@link RSocket} that intercepts interactions and gathers Micrometer metrics
 * about them.
 *
 * <p>The metrics are called {@code rsocket.[ metadata.push | request.channel | request.fnf |
 * request.response | request.stream ]} and is tagged with {@code signal.type} ({@link SignalType})
 * and any additional configured toIterable.
 *
 * @see <a href="https://micrometer.io">Micrometer</a>
 */
final class MicrometerRSocket implements RSocket {

  private final RSocket delegate;

  private final InteractionCounters metadataPush;

  private final InteractionCounters requestChannel;

  private final InteractionCounters requestFireAndForget;

  private final InteractionTimers requestResponse;

  private final InteractionCounters requestStream;

  /**
   * Creates a new {@link RSocket}.
   *
   * @param delegate the {@link RSocket} to delegate to
   * @param meterRegistry the {@link MeterRegistry} to use
   * @param tags additional toIterable to attach to {@link Meter}s
   * @throws NullPointerException if {@code delegate} or {@code meterRegistry} is {@code null}
   */
  MicrometerRSocket(RSocket delegate, MeterRegistry meterRegistry, Tag... tags) {
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");

    this.metadataPush = new InteractionCounters(meterRegistry, "metadata.push", tags);
    this.requestChannel = new InteractionCounters(meterRegistry, "request.channel", tags);
    this.requestFireAndForget = new InteractionCounters(meterRegistry, "request.fnf", tags);
    this.requestResponse = new InteractionTimers(meterRegistry, "request.response", tags);
    this.requestStream = new InteractionCounters(meterRegistry, "request.stream", tags);
  }

  @Override
  public void dispose() {
    delegate.dispose();
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return delegate.fireAndForget(payload).doFinally(requestFireAndForget);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return delegate.metadataPush(payload).doFinally(metadataPush);
  }

  @Override
  public Mono<Void> onClose() {
    return delegate.onClose();
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return delegate.requestChannel(payloads).doFinally(requestChannel);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return Mono.defer(
        () -> {
          Sample sample = requestResponse.start();

          return delegate
              .requestResponse(payload)
              .doFinally(signalType -> requestResponse.accept(sample, signalType));
        });
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return delegate.requestStream(payload).doFinally(requestStream);
  }

  private static final class InteractionCounters implements Consumer<SignalType> {

    private final Counter cancel;

    private final Counter onComplete;

    private final Counter onError;

    private InteractionCounters(MeterRegistry meterRegistry, String interactionModel, Tag... tags) {
      this.cancel = counter(meterRegistry, interactionModel, CANCEL, tags);
      this.onComplete = counter(meterRegistry, interactionModel, ON_COMPLETE, tags);
      this.onError = counter(meterRegistry, interactionModel, ON_ERROR, tags);
    }

    @Override
    public void accept(SignalType signalType) {
      switch (signalType) {
        case CANCEL:
          cancel.increment();
          break;
        case ON_COMPLETE:
          onComplete.increment();
          break;
        case ON_ERROR:
          onError.increment();
          break;
      }
    }

    private static Counter counter(
        MeterRegistry meterRegistry, String interactionModel, SignalType signalType, Tag... tags) {

      return meterRegistry.counter(
          "rsocket." + interactionModel, Tags.of(tags).and("signal.type", signalType.name()));
    }
  }

  private static final class InteractionTimers implements BiConsumer<Sample, SignalType> {

    private final Timer cancel;

    private final MeterRegistry meterRegistry;

    private final Timer onComplete;

    private final Timer onError;

    private InteractionTimers(MeterRegistry meterRegistry, String interactionModel, Tag... tags) {
      this.meterRegistry = meterRegistry;

      this.cancel = timer(meterRegistry, interactionModel, CANCEL, tags);
      this.onComplete = timer(meterRegistry, interactionModel, ON_COMPLETE, tags);
      this.onError = timer(meterRegistry, interactionModel, ON_ERROR, tags);
    }

    @Override
    public void accept(Sample sample, SignalType signalType) {
      switch (signalType) {
        case CANCEL:
          sample.stop(cancel);
          break;
        case ON_COMPLETE:
          sample.stop(onComplete);
          break;
        case ON_ERROR:
          sample.stop(onError);
          break;
      }
    }

    Sample start() {
      return Timer.start(meterRegistry);
    }

    private static Timer timer(
        MeterRegistry meterRegistry, String interactionModel, SignalType signalType, Tag... tags) {

      return meterRegistry.timer(
          "rsocket." + interactionModel, Tags.of(tags).and("signal.type", signalType.name()));
    }
  }
}
