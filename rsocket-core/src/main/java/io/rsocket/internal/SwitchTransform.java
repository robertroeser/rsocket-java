package io.rsocket.internal;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;

public final class SwitchTransform<T, R> extends Flux<R> {
    
    final Publisher<? extends T> source;
    final BiFunction<T, Flux<T>, Publisher<? extends R>> transformer;
    
    public SwitchTransform(
                              Publisher<? extends T> source, BiFunction<T, Flux<T>, Publisher<? extends R>> transformer) {
        this.source = Objects.requireNonNull(source, "source");
        this.transformer = Objects.requireNonNull(transformer, "transformer");
    }
    
    @Override
    public void subscribe(CoreSubscriber<? super R> actual) {
        Flux.from(source).subscribe(new SwitchTransformSubscriber<>(actual, transformer));
    }
    
    static final class SwitchTransformSubscriber<T, R> implements CoreSubscriber<T> {
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<SwitchTransformSubscriber> ONCE =
            AtomicIntegerFieldUpdater.newUpdater(SwitchTransformSubscriber.class, "once");
        
        final CoreSubscriber<? super R> actual;
        final BiFunction<T, Flux<T>, Publisher<? extends R>> transformer;
        final UnboundedProcessor<T> processor = new UnboundedProcessor<>();
        Subscription s;
        volatile int once;
        
        SwitchTransformSubscriber(
                                     CoreSubscriber<? super R> actual,
                                     BiFunction<T, Flux<T>, Publisher<? extends R>> transformer) {
            this.actual = actual;
            this.transformer = transformer;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.s, s)) {
                this.s = s;
                processor.onSubscribe(s);
            }
        }
        
        @Override
        public void onNext(T t) {
            if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
                try {
                    Publisher<? extends R> result =
                        Objects.requireNonNull(
                            transformer.apply(t, processor), "The transformer returned a null value");
                    Flux.from(result).subscribe(actual);
                } catch (Throwable e) {
                    onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
                    return;
                }
            }
            processor.onNext(t);
        }
        
        @Override
        public void onError(Throwable t) {
            processor.onError(t);
        }
        
        @Override
        public void onComplete() {
            processor.onComplete();
        }
    }
}