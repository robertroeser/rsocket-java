package io.rsocket.frame.ext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TagsFlyweightTest {
  private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

  @Test
  public void testToMap() {
    ByteBuf b1 = TagFlyweight.encode(allocator, "noValue");
    ByteBuf b2 = TagFlyweight.encode(allocator, "value", "hi");
    ByteBuf tags = TagsFlyweight.encode(allocator, b1, b2);
    Map<String, String> map = TagsFlyweight.toMap(tags);

    Assert.assertTrue(map.containsKey("noValue"));
    Assert.assertTrue(map.containsKey("value"));
    Assert.assertEquals("hi", map.get("value"));
  }

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
    List<ByteBuf> tags = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      ByteBuf tag = TagFlyweight.encode(allocator, "key-" + i);
      tags.add(tag);
    }

    ByteBuf encodedRoutes = TagsFlyweight.encode(allocator, tags.toArray(new ByteBuf[n]));
    int found = 0;
    for (ByteBuf b : TagsFlyweight.toIterable(encodedRoutes)) {
      String tag = TagFlyweight.key(b);
      Assert.assertEquals("key-" + found, tag);
      found++;
    }

    Assert.assertEquals(n, found);

    encodedRoutes.release();
  }
}
