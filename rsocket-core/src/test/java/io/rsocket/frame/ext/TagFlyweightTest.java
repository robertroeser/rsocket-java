package io.rsocket.frame.ext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class TagFlyweightTest {
  private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

  @Test(expected = IllegalArgumentException.class)
  public void testShouldThrowExceptionWhenKeyIsLongerThan7Bits() {
    byte[] array = new byte[128];
    new Random().nextBytes(array);
    String key = ByteBufUtil.hexDump(array);
    TagFlyweight.encode(allocator, key);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testShoudThrowExceptionWhenValueIsLongerThan8Bits() {
    byte[] array = new byte[128];
    new Random().nextBytes(array);
    String value = ByteBufUtil.hexDump(array);
    TagFlyweight.encode(allocator, "aTag", value);
  }
  
  @Test
  public void testEncodeAndDecodeKeyWithoutValue() {
    ByteBuf aTag = TagFlyweight.encode(allocator, "aTag");

    String tag = TagFlyweight.key(aTag);
  
    Assert.assertFalse(TagFlyweight.hasValue(aTag));
    Assert.assertEquals("aTag", tag);
    aTag.release();
  }
  
  
  @Test
  public void testEncodeAndDecodeWithValue() {
    ByteBuf aTag = TagFlyweight.encode(allocator, "aTag", "aValue");
    
    String tag = TagFlyweight.key(aTag);
    String value = TagFlyweight.value(aTag);
  
    Assert.assertTrue(TagFlyweight.hasValue(aTag));
    Assert.assertEquals("aTag", tag);
    Assert.assertEquals("aValue", value);
    aTag.release();
    
  }
}
