package io.rsocket.frame.ext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.*;

public class TagsFlyweight {
  public static ByteBuf encode(ByteBufAllocator allocator, ByteBuf... tags) {
    Objects.requireNonNull(allocator);
    Objects.requireNonNull(tags);

    if (tags.length == 1) {
      return tags[0];
    } else {
      return allocator.compositeBuffer().addComponents(true, tags);
    }
  }

  public static Iterable<ByteBuf> toIterable(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf);

    if (byteBuf.readableBytes() == 0) {
      return Collections.EMPTY_LIST;
    } else {
      return () ->
          new Iterator<ByteBuf>() {
            ByteBuf slice = byteBuf.slice();
            int index = slice.readerIndex();
            int readableBytes = slice.readableBytes();
            boolean hasNext = true;

            @Override
            public boolean hasNext() {
              return hasNext;
            }

            @Override
            public ByteBuf next() {
              int length = TagFlyweight.length(slice);
              index += length;

              if (readableBytes == index) {
                hasNext = false;
                return slice;
              } else {
                return slice.readSlice(length);
              }
            }
          };
    }
  }

  public static Map<String, String> toMap(ByteBuf byteBuf) {
    Map<String, String> map = new HashMap<>();
    for (ByteBuf b : toIterable(byteBuf)) {
      String key = TagFlyweight.key(b);
      String value = null;
      if (TagFlyweight.hasValue(b)) {
        value = TagFlyweight.value(b);
      }
      map.put(key, value);
    }
    return map;
  }
}
