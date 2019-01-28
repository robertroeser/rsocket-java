package io.rsocket.frame.ext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.junit.Assert;
import org.junit.Test;

public class CompositeMetadataFlyweightTest {
private static ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;;
  @Test
  public void testEncodeWellKnownType() {
    
    ByteBuf string = ByteBufUtil.writeUtf8(allocator, "Hello");

    ByteBuf encode =
        CompositeMetadataFlyweight.encode(
          allocator, WellKnownMimeType.text_plain, string);

    Assert.assertEquals(encode.readableBytes(), CompositeMetadataFlyweight.length(encode));
    Assert.assertTrue(CompositeMetadataFlyweight.hasWellknownMimeType(encode));
    Assert.assertEquals(WellKnownMimeType.text_plain, CompositeMetadataFlyweight.wellKnownMimeType(encode));
    Assert.assertEquals("Hello", CompositeMetadataFlyweight.utf8Payload(encode));
    
    encode.release();
  }

  @Test
  public void testEncodeUnknownType() {
    ByteBuf string = ByteBufUtil.writeUtf8(allocator, "Hello");

    ByteBuf encode =
        CompositeMetadataFlyweight.encode(allocator, "foo/type", string);

    Assert.assertEquals(encode.readableBytes(), CompositeMetadataFlyweight.length(encode));

    String s = CompositeMetadataFlyweight.mimeType(encode);
    Assert.assertEquals("foo/type", s);
    Assert.assertFalse(CompositeMetadataFlyweight.hasWellknownMimeType(encode));
    Assert.assertEquals("Hello", CompositeMetadataFlyweight.utf8Payload(encode));
    
    encode.release();
  }
}
