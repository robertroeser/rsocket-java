package io.rsocket.frame.ext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.rsocket.util.NumberUtils;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Flyweight to encode and decode a mime-type for an RPC style call
 *
 * @see WellKnownMimeType message_x_rsocket_rpc_v0
 */
public class RpcFlyweight {
  /**
   * Encodes an RPC call
   * @param allocator ByteBuf allocator to use
   * @param version the version of the RPC you want to call
   * @param service the service you want to call
   * @param method the method you want to call
   * @return encoded ByteBuf
   */
  public static ByteBuf encode(
      ByteBufAllocator allocator, int version, String service, String method) {
    Objects.requireNonNull(allocator, "allocator required");
    Objects.requireNonNull(service, "service required");
    Objects.requireNonNull(method, "method required");
    
    if (version > Short.MAX_VALUE) {
      throw new IllegalArgumentException("version must be less than " + Short.MAX_VALUE);
    }
    
    ByteBuf byteBuf = allocator.buffer().writeShort(version);
    int serviceLength = NumberUtils.requireUnsignedShort(ByteBufUtil.utf8Bytes(service));
    byteBuf.writeShort(serviceLength);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, service, serviceLength);

    int methodLength = NumberUtils.requireUnsignedShort(ByteBufUtil.utf8Bytes(method));
    byteBuf.writeShort(methodLength);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, method, methodLength);

    return byteBuf;
  }

  public static int version(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int i = byteBuf.readShort() & 0x7FFF;
    byteBuf.resetReaderIndex();
    return i;
  }

  public static String service(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(Short.BYTES);

    int length = byteBuf.readShort();
    String s = byteBuf.readSlice(length).toString(StandardCharsets.UTF_8);
    byteBuf.resetReaderIndex();

    return s;
  }

  public static String method(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(Short.BYTES);

    int length = byteBuf.readShort();
    length = byteBuf.skipBytes(length).readShort();
    String s = byteBuf.readSlice(length).toString(StandardCharsets.UTF_8);
    byteBuf.resetReaderIndex();

    return s;
  }
}
