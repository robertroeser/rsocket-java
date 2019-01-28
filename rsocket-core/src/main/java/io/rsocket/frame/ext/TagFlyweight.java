package io.rsocket.frame.ext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/*
 
     0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |v|Key Length   |              Key                             ...
   +---------------+-----------------------------------------------+
   |  Value Length |              Value                           ...
   +---------------+-----------------------------------------------+

   1-bit: has-value
   7-bit: key length
   UTF-8 String key
   8-bit: value
   byte field up to 8-bits long
*/

public class TagFlyweight {
  private static final int VALUE_LENGTH_MASK = 0xFF;
  private static final int KEY_LENGTH_MASK = 0x7F;
  private static final int HAS_VALUE_MASK = 0x80;

  public static ByteBuf encode(ByteBufAllocator allocator, String key) {
    return doEncode(allocator, key, null);
  }

  public static ByteBuf encode(ByteBufAllocator allocator, String key, String value) {
    Objects.requireNonNull(value);
    return doEncode(allocator, key, value);
  }

  private static ByteBuf doEncode(ByteBufAllocator allocator, String key, String value) {
    Objects.requireNonNull(allocator);
    Objects.requireNonNull(key);

    boolean hasValue = value != null;

    int keyLength = ByteBufUtil.utf8Bytes(key);

    if ((keyLength & ~KEY_LENGTH_MASK) != 0) {
      throw new IllegalArgumentException("key longer than 7 bits");
    }

    if (hasValue) {
      keyLength |= HAS_VALUE_MASK;
    }

    ByteBuf byteBuf = allocator.buffer().writeByte(keyLength);
    byteBuf.writeCharSequence(key, StandardCharsets.UTF_8);

    if (hasValue) {
      int valueLength = ByteBufUtil.utf8Bytes(value);

      if ((valueLength & ~VALUE_LENGTH_MASK) != 0) {
        throw new IllegalArgumentException("value longer than 8 bits");
      }

      byteBuf.writeShort(valueLength).writeCharSequence(value, StandardCharsets.UTF_8);
    }

    return byteBuf;
  }

  private static int readKeyLength(ByteBuf byteBuf) {
    int length = byteBuf.readUnsignedByte();

    if ((length & HAS_VALUE_MASK) == HAS_VALUE_MASK) {
      length &= ~HAS_VALUE_MASK;
    }

    return length;
  }

  public static String key(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int length = readKeyLength(byteBuf);
    String s = byteBuf.readSlice(length).toString(StandardCharsets.UTF_8);
    byteBuf.resetReaderIndex();
    return s;
  }

  public static boolean hasValue(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    boolean b = doHasValue(byteBuf);
    byteBuf.resetReaderIndex();
    return b;
  }
  
  private static boolean doHasValue(ByteBuf byteBuf) {
    return (byteBuf.readUnsignedByte() & HAS_VALUE_MASK) == HAS_VALUE_MASK;
  }
  
  public static String value(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(readKeyLength(byteBuf) + Byte.BYTES);
    int length = byteBuf.readByte();
    String s = byteBuf.readSlice(length).toString(StandardCharsets.UTF_8);
    byteBuf.resetReaderIndex();
    return s;
  }
  
  static int length(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int length = readKeyLength(byteBuf);
    
    if (doHasValue(byteBuf)) {
      byteBuf.skipBytes(length);
      length += Byte.BYTES + byteBuf.readByte();
    }
    
    byteBuf.resetReaderIndex();
    return Byte.BYTES + length;
  }
}
