package io.rsocket.frame.ext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Flyweight to encode and decode a URI style path, e.g. /person, /address
 *
 * @see WellKnownMimeType message_x_rsocket_path_v0
 */
public class PathFlyweight {
  public static ByteBuf encode(ByteBufAllocator allocator, String path) {
    Objects.requireNonNull(allocator, "allocator required");
    Objects.requireNonNull(path, "path required");
    return ByteBufUtil.writeUtf8(allocator, path);
  }

  public static String path(ByteBuf byteBuf) {
    return byteBuf.toString(StandardCharsets.UTF_8);
  }
}
