/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import reactor.core.publisher.*;
import reactor.ipc.netty.ByteBufFlux;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.NettyInbound;
import reactor.ipc.netty.NettyOutbound;

public class NettyDuplexConnection implements DuplexConnection {
  private final NettyInbound in;
  private final NettyOutbound out;
  private final NettyContext context;
  private final MonoProcessor<Void> onClose;
  private final UnicastProcessor<Frame> sendProcessor;
  
  public NettyDuplexConnection(NettyInbound in, NettyOutbound out, NettyContext context) {
    this.in = in;
    this.out = out;
    this.context = context;
    this.onClose = MonoProcessor.create();
    this.sendProcessor = UnicastProcessor.create();

    context.onClose(onClose::onComplete);
    this.onClose
        .doFinally(
            s -> {
              this.context.dispose();
              this.context.channel().close();
            })
        .subscribe();

    sendProcessor
        .map(Frame::content)
        .concatMap(out::sendObject).then().subscribe();

    //sendProcessor.subscribe(out::sendObject);
    //Flux.from(out.send(sendProcessor.doOnRequest(l -> System.out.println(l)))).subscribe();
  }
  
  @Override
  public Mono<Void> send(Publisher<Frame> frames) {
    Flux.from(frames).doOnNext(sendProcessor::onNext).subscribe();
    return Mono.empty();
    //return Flux.from(frames).concatMap(this::sendOne).then();
  }
  
  @Override
  public Mono<Void> sendOne(Frame frame) {
    //return out.sendObject(frame.content()).then();
    sendProcessor.onNext(frame);
    return Mono.empty();
  }
  
  @Override
  public Flux<Frame> receive() {
    return in.receive().map(buf -> Frame.from(buf.retain()));
  }
  
  @Override
  public Mono<Void> close() {
    return Mono.fromRunnable(onClose::onComplete);
  }
  
  @Override
  public Mono<Void> onClose() {
    return onClose;
  }
  
  @Override
  public double availability() {
    return onClose.isTerminated() ? 0.0 : 1.0;
  }
}
