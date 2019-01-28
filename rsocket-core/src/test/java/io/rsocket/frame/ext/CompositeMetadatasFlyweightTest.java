package io.rsocket.frame.ext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CompositeMetadatasFlyweightTest {
  private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

  @Test
  public void testEncodeNoValue_1_Route() {
    testEncodeNoValue_n_Routes(1);
  }

  @Test
  public void testEncodeNoValue_10_Route() {
    testEncodeNoValue_n_Routes(10);
  }

  @Test
  public void testEncodeNoValue_1000_Route() {
    testEncodeNoValue_n_Routes(1000);
  }

  public void testEncodeNoValue_n_Routes(int n) {
    List<ByteBuf> metadatas = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      ByteBuf string = ByteBufUtil.writeUtf8(allocator, "Hello");
      ByteBuf encode =
          CompositeMetadataFlyweight.encode(allocator, WellKnownMimeType.text_plain, string);
      metadatas.add(encode);
    }

    ByteBuf encodedRoutes =
        CompositeMetadatasFlyweight.encode(allocator, metadatas.toArray(new ByteBuf[n]));
    int found = 0;
    for (ByteBuf encode : CompositeMetadatasFlyweight.toIterable(encodedRoutes)) {
      Assert.assertEquals(encode.readableBytes(), CompositeMetadataFlyweight.length(encode));
      Assert.assertTrue(CompositeMetadataFlyweight.hasWellknownMimeType(encode));
      Assert.assertEquals(
          WellKnownMimeType.text_plain, CompositeMetadataFlyweight.wellKnownMimeType(encode));
      Assert.assertEquals("Hello", CompositeMetadataFlyweight.utf8Payload(encode));

      found++;
    }

    Assert.assertEquals(n, found);

    encodedRoutes.release();
  }

  @Test
  public void testUsage() {
    ByteBuf tags =
        TagsFlyweight.encode(
            allocator,
            TagFlyweight.encode(allocator, "region", "us-west-1"),
            TagFlyweight.encode(allocator, "zone", "a"),
            TagFlyweight.encode(allocator, "version", "1.2.3"));

    ByteBuf rpc = RpcFlyweight.encode(allocator, 1, "io.rsocket.test.Service", "StreamMessages");

    ByteBuf tracing = TracingMetadataFlyweight.encode64Bit(allocator, true, true, true, true, 1, 2);

    ByteBuf metadatas =
        CompositeMetadatasFlyweight.encode(
            allocator,
          CompositeMetadataFlyweight.encode(
            allocator, WellKnownMimeType.message_x_rsocket_rpc_v0, rpc),
            CompositeMetadataFlyweight.encode(
                allocator, WellKnownMimeType.message_x_rsocket_tags_v0, tags),
            CompositeMetadataFlyweight.encode(
                allocator, WellKnownMimeType.message_x_rsocket_tracing_zipkin_v0, tracing));

    for (ByteBuf encode : CompositeMetadatasFlyweight.toIterable(metadatas)) {
      if (CompositeMetadataFlyweight.hasWellknownMimeType(encode)
          && CompositeMetadataFlyweight.wellKnownMimeType(encode)
              .equals(WellKnownMimeType.message_x_rsocket_rpc_v0)) {
        ByteBuf payload = CompositeMetadataFlyweight.payload(encode);
        Assert.assertEquals("StreamMessages", RpcFlyweight.method(payload));
      }
    }
  }
}
