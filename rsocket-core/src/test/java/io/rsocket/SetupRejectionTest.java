package io.rsocket;

import static io.rsocket.transport.ServerTransport.*;
import static org.assertj.core.api.Assertions.*;

import io.rsocket.exceptions.Exceptions;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.framing.FrameType;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.transport.ServerTransport;
import io.rsocket.util.DefaultPayload;

import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;

public class SetupRejectionTest {

  @Test
  void responderRejectSetup() {
    SingleConnectionTransport transport = new SingleConnectionTransport();

    String errorMsg = "error";
    RejectingAcceptor acceptor = new RejectingAcceptor(errorMsg);
    RSocketFactory.receive().acceptor(acceptor).transport(transport).start().block();

    transport.connect();

    Frame sentFrame = transport.awaitSent();
    assertThat(sentFrame.getType()).isEqualTo(FrameType.ERROR);
    RuntimeException error = Exceptions.from(sentFrame);
    assertThat(errorMsg).isEqualTo(error.getMessage());
    assertThat(error).isInstanceOf(RejectedSetupException.class);
    RSocket acceptorSender = acceptor.senderRSocket().block();
    assertThat(acceptorSender.isDisposed()).isTrue();
  }

  @Test
  void requesterStreamsTerminatedOnZeroErrorFrame() {
    TestDuplexConnection conn = new TestDuplexConnection();
    List<Throwable> errors = new ArrayList<>();
    RSocketClient rSocket =
        new RSocketClient(
            conn, DefaultPayload::create, errors::add, StreamIdSupplier.clientSupplier());

    String errorMsg = "error";

    Mono.delay(Duration.ofMillis(100))
        .doOnTerminate(
            () ->
                conn.addToReceivedBuffer(Frame.Error.from(0, new RejectedSetupException(errorMsg))))
        .subscribe();

    StepVerifier.create(rSocket.requestResponse(DefaultPayload.create("test")))
        .expectErrorMatches(
            err ->
            {
              return (err instanceof RejectedSetupException && errorMsg.equals(err.getMessage())) || (err instanceof ClosedChannelException);
            })
        .verify(Duration.ofSeconds(5));

    assertThat(errors).hasSize(1);
    assertThat(rSocket.isDisposed()).isTrue();
  }

  @Test
  void requesterNewStreamsTerminatedAfterZeroErrorFrame() {
    TestDuplexConnection conn = new TestDuplexConnection();
    RSocketClient rSocket =
        new RSocketClient(
            conn, DefaultPayload::create, err -> {}, StreamIdSupplier.clientSupplier());

    conn.addToReceivedBuffer(Frame.Error.from(0, new RejectedSetupException("error")));

    StepVerifier.create(
            rSocket
                .requestResponse(DefaultPayload.create("test"))
                .delaySubscription(Duration.ofMillis(100)))
        .expectErrorMatches(
            err -> err instanceof ClosedChannelException)
        .verify(Duration.ofSeconds(5));
  }

  private static class RejectingAcceptor implements SocketAcceptor {
    private final String errorMessage;

    public RejectingAcceptor(String errorMessage) {
      this.errorMessage = errorMessage;
    }

    private final UnicastProcessor<RSocket> senderRSockets = UnicastProcessor.create();

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
      senderRSockets.onNext(sendingSocket);
      return Mono.error(new RuntimeException(errorMessage));
    }

    public Mono<RSocket> senderRSocket() {
      return senderRSockets.next();
    }
  }

  private static class SingleConnectionTransport implements ServerTransport<TestCloseable> {

    private final TestDuplexConnection conn = new TestDuplexConnection();

    @Override
    public Mono<TestCloseable> start(ConnectionAcceptor acceptor) {
      return Mono.just(new TestCloseable(acceptor, conn));
    }

    public Frame awaitSent() {
      try {
        return conn.awaitSend();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public void connect() {
      Frame setup =
          Frame.Setup.from(
              0, 42, 1, "mdMime", "dMime", DefaultPayload.create(DefaultPayload.EMPTY_BUFFER));
      conn.addToReceivedBuffer(setup);
    }
  }

  private static class TestCloseable implements Closeable {

    private final DuplexConnection conn;

    TestCloseable(ConnectionAcceptor acceptor, DuplexConnection conn) {
      this.conn = conn;
      Mono.from(acceptor.apply(conn)).subscribe(notUsed -> {}, err -> conn.dispose());
    }

    @Override
    public Mono<Void> onClose() {
      return conn.onClose();
    }

    @Override
    public void dispose() {
      conn.dispose();
    }
  }
}
