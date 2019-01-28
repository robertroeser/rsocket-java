package io.rsocket.frame.ext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

public class RpcFlyweightTest {

  private static ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

  @Test
  public void testEncodeAndDecode() {
    short version = 1;
    String service = "aService";
    String method = "aMethod";
  
    ByteBuf encode = RpcFlyweight
                       .encode(allocator, version, service, method);

    Assert.assertEquals(version, RpcFlyweight.version(encode));
    Assert.assertEquals(service, RpcFlyweight.service(encode));
    Assert.assertEquals(method, RpcFlyweight.method(encode));
  }
  
  @Test(expected = NullPointerException.class)
  public void testShouldThrowErrorWhenServiceMissing() {
    short version = 1;
    String service = null;
    String method = "aMethod";
  
    ByteBuf encode = RpcFlyweight
                       .encode(allocator, version, service, method);
  }
  
  @Test(expected = NullPointerException.class)
  public void testShouldThrowErrorWhenMethodMissing() {
  
    short version = 1;
    String service = "aService";
    String method = null;
  
    ByteBuf encode = RpcFlyweight
                       .encode(allocator, version, service, method);
  }
}
