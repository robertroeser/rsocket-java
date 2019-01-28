package io.rsocket.frame.ext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

public class PathFlyweightTest {
  
  private static ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
  
  @Test
  public void testEnode() {
    ByteBuf encode = PathFlyweight
                       .encode(allocator, "/path");
  
    Assert.assertEquals("/path", PathFlyweight.path(encode));
    encode.release();
  }
  
  @Test(expected = NullPointerException.class)
  public void testPathRequired() {
    PathFlyweight
      .encode(allocator, null);
  }
}