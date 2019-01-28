package io.rsocket.frame.ext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Enumeration of well known mime-types. Maps a well known mine-type to a code when encoding it in
 * the metadata extensions.
 */
public enum WellKnownMimeType {
  application_avro("application/avro", 0),
  application_cbor("application/cbor", 1),
  application_graphql("application/graphql", 2),
  application_gzip("application/gzip", 3),
  application_javascript("application/javascript", 4),
  application_json("application/json", 5),
  application_octet_stream("application/octet-stream", 6),
  application_pdf("application/pdf", 7),
  application_vnd_apache_thrift_binary("application/vnd.apache.thrift.binary", 8),
  application_vnd_google_protobuf("application/vnd.google.protobuf", 9),
  application_xml("application/xml", 10),
  application_zip("application/zip", 11),
  audio_aac("audio/aac", 12),
  audio_mp3("audio/mp3", 13),
  audio_mp4("audio/mp4", 14),
  audio_mpeg3("audio/mpeg3", 15),
  audio_mpeg("audio/mpeg", 16),
  audio_ogg("audio/ogg", 17),
  audio_opus("audio/opus", 18),
  audio_vorbis("audio/vorbis", 19),
  image_bmp("image/bmp", 20),
  image_gif("image/gif", 21),
  image_heic_sequence("image/heic-sequence", 22),
  image_heic("image/heic", 23),
  image_heif_sequence("image/heif-sequence", 24),
  image_heif("image/heif", 25),
  image_jpeg("image/jpeg", 26),
  image_png("image/png", 27),
  image_tiff("image/tiff", 28),
  multipart_mixed("multipart/mixed", 29),
  text_css("text/css", 30),
  text_csv("text/csv", 31),
  text_html("text/html", 32),
  text_plain("text/plain", 33),
  text_xml("text/xml", 34),
  video_H264("video/H264", 35),
  video_H265("video/H265", 36),
  video_VP8("video/VP8", 37),

  // RSocket Types
  message_x_rsocket_tags_v0("message/x.rsocket.toIterable.v0", 123),
  message_x_rsocket_path_v0("message/x.rsocket.path.v0", 124),
  message_x_rsocket_rpc_v0("message/x.rsocket.rpc.v0", 125),
  message_x_rsocket_tracing_zipkin_v0("message/x.rsocket.tracing-zipkin.v0", 126),
  message_x_rsocket_composite_metadata_v0("message/x.rsocket.composite-metadata.v0", 127);
  
  private static final WellKnownMimeType[] MIME_TYPES_BY_CODE;
  private static final Map<String, WellKnownMimeType> MIME_TYPES_BY_STRING;

  static {
    MIME_TYPES_BY_CODE = new WellKnownMimeType[getMaxiumWellKnownTypes() + 1];
    MIME_TYPES_BY_STRING = new HashMap<>(MIME_TYPES_BY_CODE.length);

    for (WellKnownMimeType type : values()) {
      MIME_TYPES_BY_CODE[type.code] = type;
      MIME_TYPES_BY_STRING.put(type.getMimeType(), type);
    }
  }
  
  private final String mimeType;
  private final int code;

  WellKnownMimeType(String mimeType, int code) {
    this.mimeType = mimeType;
    this.code = code;
  }

  private static int getMaxiumWellKnownTypes() {
    return Arrays.stream(values()).mapToInt(frameType -> frameType.code).max().orElse(0);
  }

  /**
   * @param mimeType A mime-type in string from
   * @return A well known mime-type that maps to the string provided
   * @throws IllegalArgumentException Throws an exception if there is no mime-type
   */
  public static WellKnownMimeType fromMimeType(String mimeType) throws IllegalArgumentException {
    WellKnownMimeType type = MIME_TYPES_BY_STRING.get(mimeType);

    if (mimeType == null) {
      throw new IllegalArgumentException("no mime-type found for " + mimeType);
    }

    return type;
  }

  public static WellKnownMimeType fromCode(int code) {
    return MIME_TYPES_BY_CODE[code];
  }

  public boolean contains(String mimeType) {
    return MIME_TYPES_BY_STRING.containsKey(mimeType);
  }

  public String getMimeType() {
    return mimeType;
  }

  public int getCode() {
    return code;
  }
}
