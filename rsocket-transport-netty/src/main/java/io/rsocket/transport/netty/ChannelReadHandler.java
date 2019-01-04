package io.rsocket.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Frame;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

public class ChannelReadHandler extends ChannelInboundHandlerAdapter {
  private Channel channel;
  private final Flux<Frame> frames;
  private final UnicastProcessor<Frame> processor;

  private long requested;

  private boolean reading = false;

  public ChannelReadHandler(Channel channel) {
    this.channel = channel;
    this.channel.config().setAutoRead(false);
    this.processor = UnicastProcessor
        .create(Queues.<Frame>one().get());
    this.frames = processor
        .doOnRequest(this::doOnRequest)
        .doOnNext(frame -> doOnNext());

    channel
        .newProgressivePromise();

  }

  private void doOnRequest(long request) {
    tryRequestMore(channel, request);
  }

  private void doOnNext() {
    tryRequestMore(channel, -1);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    try {
      ByteBuf buffer = (ByteBuf) msg;
      processor.onNext(Frame.from(buffer));
    } catch (Throwable t) {
      Exceptions.throwIfFatal(t);
      ReferenceCountUtil.safeRelease(msg);
      ctx.fireExceptionCaught(t);
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    reading = false;
    tryRequestMore(channel, 0);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    processor.onComplete();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    processor.onError(cause);
  }

  private void tryRequestMore(Channel channel, long delta) {
    if (channel.eventLoop().inEventLoop()) {
      requested = Operators.addCap(requested, delta);
      if (requested > 0 && !reading) {
        reading = true;
        channel.read();
      }
    } else {
      channel
          .newPromise()
          .addListener(future ->
              tryRequestMore(channel, delta)
          );
    }
  }

  public Flux<Frame> getProcessor() {
    return frames;
  }
}