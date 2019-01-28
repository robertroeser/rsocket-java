package io.rsocket.frame.ext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;

import java.nio.charset.StandardCharsets;

/**
 * Implementation of the Composite Metadata Spec:
 * https://github.com/rsocket/rsocket/blob/master/Extensions/CompositeMetadata.md
 */
public class CompositeMetadataFlyweight {
  private static final int FRAME_LENGTH_MASK = 0xFFFFFF;
  private static final int MIME_TYPE_LENGTH_MASK = 0x7F;
  private static final int WELL_KNOWN_MASK = 0x80;
  private static final int METADATA_SIZE_LENGTH = 3;
  private static final int NOT_MIME_TYPE_LENGTH_MASK = ~MIME_TYPE_LENGTH_MASK;
  private static final int NOT_FRAME_LENGTH_MASK = ~FRAME_LENGTH_MASK;
  private static final int NOT_WELL_KNOWN_MASK = ~WELL_KNOWN_MASK;

  public static ByteBuf encode(
      ByteBufAllocator allocator, WellKnownMimeType type, ByteBuf metadata) {
    int length = metadata.readableBytes();
    int code = type.getCode();

    ByteBuf byteBuf = allocator.buffer().writeByte(WELL_KNOWN_MASK | code);

    encodeLength(byteBuf, length);

    return allocator.compositeBuffer().addComponents(true, byteBuf, metadata);
  }

  public static ByteBuf encode(ByteBufAllocator allocator, String type, ByteBuf metadata) {
    int length = ByteBufUtil.utf8Bytes(type);

    if ((length & NOT_MIME_TYPE_LENGTH_MASK) != 0) {
      throw new IllegalArgumentException("mime type is longer than 7 bits");
    }

    ByteBuf byteBuf = allocator.buffer().writeByte(length);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, type, length);
    encodeLength(byteBuf, metadata.readableBytes());
    return allocator.compositeBuffer().addComponents(true, byteBuf, metadata);
  }

  private static void encodeLength(final ByteBuf byteBuf, final int length) {
    if ((length & NOT_FRAME_LENGTH_MASK) != 0) {
      throw new IllegalArgumentException("Length is larger than 24 bits");
    }
    // Write each byte separately in reverse order, this mean we can write 1 << 23 without
    // overflowing.
    byteBuf.writeByte(length >> 16);
    byteBuf.writeByte(length >> 8);
    byteBuf.writeByte(length);
  }

  private static int decodeLength(final ByteBuf byteBuf) {
    int length = byteBuf.readUnsignedByte() << 16;
    length |= byteBuf.readUnsignedByte() << 8;
    length |= byteBuf.readUnsignedByte();
    return length;
  }

  public static boolean hasWellknownMimeType(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    boolean b = doHasWellknowMimeType(byteBuf.readUnsignedByte());
    byteBuf.resetReaderIndex();
    return b;
  }

  private static boolean doHasWellknowMimeType(int b) {
    return (b & WELL_KNOWN_MASK) == WELL_KNOWN_MASK;
  }

  public static WellKnownMimeType wellKnownMimeType(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int i = byteBuf.readUnsignedByte();
    WellKnownMimeType mimeType = WellKnownMimeType.fromCode(i & NOT_WELL_KNOWN_MASK);
    byteBuf.resetReaderIndex();
    return mimeType;
  }

  public static String mimeType(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int length = byteBuf.readUnsignedByte() & NOT_WELL_KNOWN_MASK;
    String s = byteBuf.readSlice(length).toString(StandardCharsets.UTF_8);
    byteBuf.resetReaderIndex();
    return s;
  }

  public static ByteBuf payload(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();

    int metadataLength = byteBuf.readUnsignedByte();
    if (!doHasWellknowMimeType(metadataLength)) {
      metadataLength &= NOT_WELL_KNOWN_MASK;
      byteBuf.skipBytes(metadataLength);
    }

    int length = decodeLength(byteBuf);
    ByteBuf payload = byteBuf.readSlice(length);
    byteBuf.resetReaderIndex();
    return payload;
  }

  public static String utf8Payload(ByteBuf byteBuf) {
    return payload(byteBuf).toString(StandardCharsets.UTF_8);
  }

  static int length(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();

    int length;
    int metadataLength = byteBuf.readUnsignedByte();
    if (!doHasWellknowMimeType(metadataLength)) {
      length = (metadataLength & NOT_WELL_KNOWN_MASK);
      byteBuf.skipBytes(length);
      length += Byte.BYTES;
    } else {
      length = Byte.BYTES;
    }

    length += decodeLength(byteBuf) + METADATA_SIZE_LENGTH;
    byteBuf.resetReaderIndex();
    return length;
  }
}
