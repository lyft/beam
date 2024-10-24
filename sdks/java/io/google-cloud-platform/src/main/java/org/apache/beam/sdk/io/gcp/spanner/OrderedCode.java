/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.spanner;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.LongMath;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedInteger;

/**
 * This module provides routines for encoding a sequence of typed entities into a byte array. The
 * resulting byte arrays can be lexicographically compared to yield the same comparison value that
 * would have been generated if the encoded items had been compared one by one according to their
 * type.
 *
 * <p>More precisely, suppose: 1. byte array A is generated by encoding the sequence of items
 * [A_1..A_n] 2. byte array B is generated by encoding the sequence of items [B_1..B_n] 3. The types
 * match; i.e., for all i: A_i was encoded using the same routine as B_i
 *
 * <p>Then: Comparing A vs. B lexicographically is the same as comparing the vectors [A_1..A_n] and
 * [B_1..B_n] lexicographically.
 *
 * <p><b>This class is NOT thread safe.</b>
 */
class OrderedCode {
  // We want to encode a few extra symbols in strings:
  //      <sep>           Separator between items
  //      <infinity>      Infinite string
  //
  // Therefore we need an alphabet with at least 258 characters.  We
  // achieve this by using two-letter sequences starting with '\0' and '\xff'
  // as extra symbols:
  //      <sep>           encoded as =>           \0\1
  //      \0              encoded as =>           \0\xff
  //      \xff            encoded as =>           \xff\x00
  //      <infinity>      encoded as =>           \xff\xff
  //
  // The remaining two letter sequences starting with '\0' and '\xff'
  // are currently unused.

  static final byte ESCAPE1 = 0x00;
  static final byte NULL_CHARACTER = (byte) 0xff; // Combined with ESCAPE1
  static final byte SEPARATOR = 0x01; // Combined with ESCAPE1

  static final byte ESCAPE2 = (byte) 0xff;
  static final byte INFINITY = (byte) 0xff; // Combined with ESCAPE2
  static final byte FF_CHARACTER = 0x00; // Combined with ESCAPE2

  static final byte[] ESCAPE1_SEPARATOR = {ESCAPE1, SEPARATOR};

  static final byte[] INFINITY_ENCODED = {ESCAPE2, INFINITY};

  static final byte[] INFINITY_ENCODED_DECREASING = {invert(ESCAPE2), invert(INFINITY)};

  /**
   * This array maps encoding length to header bits in the first two bytes for SignedNumIncreasing
   * encoding.
   */
  private static final byte[][] LENGTH_TO_HEADER_BITS = {
    {0, 0},
    {(byte) 0x80, 0},
    {(byte) 0xc0, 0},
    {(byte) 0xe0, 0},
    {(byte) 0xf0, 0},
    {(byte) 0xf8, 0},
    {(byte) 0xfc, 0},
    {(byte) 0xfe, 0},
    {(byte) 0xff, 0},
    {(byte) 0xff, (byte) 0x80},
    {(byte) 0xff, (byte) 0xc0}
  };

  /**
   * This array maps encoding lengths to the header bits that overlap with the payload and need
   * fixing during readSignedNumIncreasing.
   */
  private static final long[] LENGTH_TO_MASK = {
    0L,
    0x80L,
    0xc000L,
    0xe00000L,
    0xf0000000L,
    0xf800000000L,
    0xfc0000000000L,
    0xfe000000000000L,
    0xff00000000000000L,
    0x8000000000000000L,
    0L
  };

  /**
   * This array maps the number of bits in a number to the encoding length produced by
   * WriteSignedNumIncreasing. For positive numbers, the number of bits is 1 plus the most
   * significant bit position (the highest bit position in a positive long is 63). For a negative
   * number n, we count the bits in ~n. That is, length = BITS_TO_LENGTH[log2Floor(n < 0 ? ~n : n) +
   * 1].
   */
  private static final short[] BITS_TO_LENGTH = {
    1, 1, 1, 1, 1, 1, 1,
    2, 2, 2, 2, 2, 2, 2,
    3, 3, 3, 3, 3, 3, 3,
    4, 4, 4, 4, 4, 4, 4,
    5, 5, 5, 5, 5, 5, 5,
    6, 6, 6, 6, 6, 6, 6,
    7, 7, 7, 7, 7, 7, 7,
    8, 8, 8, 8, 8, 8, 8,
    9, 9, 9, 9, 9, 9, 9,
    10
  };

  // stores the current encoded value as a list of byte arrays. Note that this
  // is manipulated as we read/write items.
  // Note that every item will fit on at most one array. One array may
  // have more than one item (eg when used for decoding). While encoding,
  // one array will have exactly one item. While returning the encoded array
  // we will merge all the arrays in this list.
  private final ArrayList<byte[]> encodedArrays = new ArrayList<>();

  // This is the current position on the first array. Will be non-zero
  // only if the ordered code was created using encoded byte array.
  private int firstArrayPosition = 0;

  /** Creates OrderedCode from scratch. Typically used at encoding time. */
  public OrderedCode() {}

  /**
   * Creates OrderedCode from a given encoded byte array. Typically used at decoding time.
   *
   * <p><b> For better performance, it uses the input array provided (not a copy). Therefore the
   * input array should not be modified.</b>
   */
  public OrderedCode(byte[] encodedByteArray) {
    encodedArrays.add(encodedByteArray);
  }

  /**
   * Adds the given byte array item to the OrderedCode. It encodes the input byte array, followed by
   * a separator and appends the result to its internal encoded byte array store.
   *
   * <p>It works with the input array, so the input array 'value' should not be modified till the
   * method returns.
   *
   * @param value bytes to be written.
   * @see #readBytes()
   */
  public void writeBytes(byte[] value) {
    writeBytes(value, false);
  }

  public void writeBytesDecreasing(byte[] value) {
    writeBytes(value, true);
  }

  private void writeBytes(byte[] value, boolean invert) {
    // Determine the length of the encoded array
    int encodedLength = 2; // for separator
    for (byte b : value) {
      if ((b == ESCAPE1) || (b == ESCAPE2)) {
        encodedLength += 2;
      } else {
        encodedLength++;
      }
    }

    byte[] encodedArray = new byte[encodedLength];
    int copyStart = 0;
    int outIndex = 0;
    for (int i = 0; i < value.length; i++) {
      byte b = value[i];
      if (b == ESCAPE1) {
        arraycopy(invert, value, copyStart, encodedArray, outIndex, i - copyStart);
        outIndex += i - copyStart;
        encodedArray[outIndex++] = convert(invert, ESCAPE1);
        encodedArray[outIndex++] = convert(invert, NULL_CHARACTER);
        copyStart = i + 1;
      } else if (b == ESCAPE2) {
        arraycopy(invert, value, copyStart, encodedArray, outIndex, i - copyStart);
        outIndex += i - copyStart;
        encodedArray[outIndex++] = convert(invert, ESCAPE2);
        encodedArray[outIndex++] = convert(invert, FF_CHARACTER);
        copyStart = i + 1;
      }
    }
    if (copyStart < value.length) {
      arraycopy(invert, value, copyStart, encodedArray, outIndex, value.length - copyStart);
      outIndex += value.length - copyStart;
    }
    encodedArray[outIndex++] = convert(invert, ESCAPE1);
    encodedArray[outIndex] = convert(invert, SEPARATOR);

    encodedArrays.add(encodedArray);
  }

  private static byte convert(boolean invert, byte val) {
    return invert ? (byte) ~val : val;
  }

  private static byte invert(byte val) {
    return convert(true, val);
  }

  private void arraycopy(
      boolean invert, byte[] src, int srcPos, byte[] dest, int destPos, int length) {
    System.arraycopy(src, srcPos, dest, destPos, length);
    if (invert) {
      for (int i = destPos; i < destPos + length; i++) {
        dest[i] = (byte) ~dest[i];
      }
    }
  }

  /**
   * Encodes the long item, in big-endian format, and appends the result to its internal encoded
   * byte array store.
   *
   * @see #readNumIncreasing()
   */
  public void writeNumIncreasing(long value) {
    // Values are encoded with a single byte length prefix, followed
    // by the actual value in big-endian format with leading 0 bytes
    // dropped.
    byte[] bufer = new byte[9]; // 8 bytes for value plus one byte for length
    int len = 0;
    while (value != 0) {
      len++;
      bufer[9 - len] = (byte) (value & 0xff);
      value >>>= 8;
    }
    bufer[9 - len - 1] = (byte) len;
    len++;
    byte[] encodedArray = new byte[len];
    System.arraycopy(bufer, 9 - len, encodedArray, 0, len);
    encodedArrays.add(encodedArray);
  }

  public void writeNumIncreasing(UnsignedInteger unsignedInt) {
    writeNumIncreasing(unsignedInt.longValue());
  }

  /**
   * Encodes the long item, in big-endian format, and appends the result to its internal encoded
   * byte array store.
   *
   * @see #readNumIncreasing()
   */
  public void writeNumDecreasing(long value) {
    // Values are encoded with a complemented single byte length prefix,
    // followed by the complement of the actual value in big-endian format with
    // leading 0xff bytes dropped.
    byte[] bufer = new byte[9]; // 8 bytes for value plus one byte for length
    int len = 0;
    while (value != 0) {
      len++;
      bufer[9 - len] = (byte) ~(value & 0xff);
      value >>>= 8;
    }
    bufer[9 - len - 1] = (byte) ~len;
    len++;
    byte[] encodedArray = new byte[len];
    System.arraycopy(bufer, 9 - len, encodedArray, 0, len);
    encodedArrays.add(encodedArray);
  }

  public void writeNumDecreasing(UnsignedInteger unsignedInt) {
    writeNumDecreasing(unsignedInt.longValue());
  }

  /** Return floor(log2(n)) for positive integer n. Returns -1 iff n == 0. */
  @VisibleForTesting
  int log2Floor(long n) {
    checkArgument(n >= 0);
    return n == 0 ? -1 : LongMath.log2(n, RoundingMode.FLOOR);
  }

  /** Calculates the encoding length in bytes of the signed number n. */
  @VisibleForTesting
  int getSignedEncodingLength(long n) {
    return BITS_TO_LENGTH[log2Floor(n < 0 ? ~n : n) + 1];
  }

  /** @see #readSignedNumIncreasing() */
  public void writeSignedNumIncreasing(long val) {
    long x = val < 0 ? ~val : val;
    if (x < 64) { // Fast path for encoding length == 1.
      byte[] encodedArray = new byte[] {(byte) (LENGTH_TO_HEADER_BITS[1][0] ^ val)};
      encodedArrays.add(encodedArray);
      return;
    }
    // buf = val in network byte order, sign extended to 10 bytes.
    byte signByte = val < 0 ? (byte) 0xff : 0;
    byte[] buf = new byte[2 + Longs.BYTES];
    buf[0] = buf[1] = signByte;
    System.arraycopy(Longs.toByteArray(val), 0, buf, 2, Longs.BYTES);
    int len = getSignedEncodingLength(x);
    if (len < 2) {
      throw new IllegalStateException(
          "Invalid length (" + len + ")" + " returned by getSignedEncodingLength(" + x + ")");
    }
    int beginIndex = buf.length - len;
    buf[beginIndex] ^= LENGTH_TO_HEADER_BITS[len][0];
    buf[beginIndex + 1] ^= LENGTH_TO_HEADER_BITS[len][1];

    byte[] encodedArray = new byte[len];
    System.arraycopy(buf, beginIndex, encodedArray, 0, len);
    encodedArrays.add(encodedArray);
  }

  public void writeSignedNumDecreasing(long val) {
    writeSignedNumIncreasing(~val);
  }

  /**
   * Encodes and appends INFINITY item to its internal encoded byte array store.
   *
   * @see #readInfinity()
   */
  public void writeInfinity() {
    writeTrailingBytes(INFINITY_ENCODED);
  }

  /**
   * Encodes and appends INFINITY item which would come before any real string.
   *
   * @see #readInfinityDecreasing()
   */
  public void writeInfinityDecreasing() {
    writeTrailingBytes(INFINITY_ENCODED_DECREASING);
  }

  /**
   * Appends the byte array item to its internal encoded byte array store. This is used for the last
   * item and is not encoded.
   *
   * <p>It stores the input array in the store, so the input array 'value' should not be modified.
   *
   * @param value bytes to be written.
   * @see #readTrailingBytes()
   */
  public void writeTrailingBytes(byte[] value) {
    if ((value == null) || (value.length == 0)) {
      throw new IllegalArgumentException("Value cannot be null or have 0 elements");
    }

    encodedArrays.add(value);
  }

  /**
   * Returns the next byte array item from its encoded byte array store and removes the item from
   * the store.
   *
   * @see #writeBytes(byte[])
   */
  public byte[] readBytes() {
    return readBytes(false);
  }

  public byte[] readBytesDecreasing() {
    return readBytes(true);
  }

  private byte[] readBytes(boolean invert) {
    if (encodedArrays.isEmpty() || (encodedArrays.get(0).length - firstArrayPosition <= 0)) {
      throw new IllegalArgumentException("Invalid encoded byte array");
    }

    // Determine the length of the decoded array
    // We only scan up to "length-2" since a valid string must end with
    // a two character terminator: 'ESCAPE1 SEPARATOR'
    byte[] store = encodedArrays.get(0);
    int decodedLength = 0;
    boolean valid = false;
    int i = firstArrayPosition;
    while (i < store.length - 1) {
      byte b = store[i++];
      if (b == convert(invert, ESCAPE1)) {
        b = store[i++];
        if (b == convert(invert, SEPARATOR)) {
          valid = true;
          break;
        } else if (b == convert(invert, NULL_CHARACTER)) {
          decodedLength++;
        } else {
          throw new IllegalArgumentException("Invalid encoded byte array");
        }
      } else if (b == convert(invert, ESCAPE2)) {
        b = store[i++];
        if (b == convert(invert, FF_CHARACTER)) {
          decodedLength++;
        } else {
          throw new IllegalArgumentException("Invalid encoded byte array");
        }
      } else {
        decodedLength++;
      }
    }
    if (!valid) {
      throw new IllegalArgumentException("Invalid encoded byte array");
    }

    byte[] decodedArray = new byte[decodedLength];
    int copyStart = firstArrayPosition;
    int outIndex = 0;
    int j = firstArrayPosition;
    while (j < store.length - 1) {
      byte b = store[j++]; // note that j has been incremented
      if (b == convert(invert, ESCAPE1)) {
        arraycopy(invert, store, copyStart, decodedArray, outIndex, j - copyStart - 1);
        outIndex += j - copyStart - 1;
        // ESCAPE1 SEPARATOR ends component
        // ESCAPE1 NULL_CHARACTER represents '\0'
        b = store[j++];
        if (b == convert(invert, SEPARATOR)) {
          if ((store.length - j) == 0) {
            // we are done with the first array
            encodedArrays.remove(0);
            firstArrayPosition = 0;
          } else {
            firstArrayPosition = j;
          }
          return decodedArray;
        } else if (b == convert(invert, NULL_CHARACTER)) {
          decodedArray[outIndex++] = 0x00;
        } // else not required - handled during length determination
        copyStart = j;
      } else if (b == convert(invert, ESCAPE2)) {
        arraycopy(invert, store, copyStart, decodedArray, outIndex, j - copyStart - 1);
        outIndex += j - copyStart - 1;
        // ESCAPE2 FF_CHARACTER represents '\xff'
        // ESCAPE2 INFINITY is an error
        b = store[j++];
        if (b == convert(invert, FF_CHARACTER)) {
          decodedArray[outIndex++] = (byte) 0xff;
        } // else not required - handled during length determination
        copyStart = j;
      }
    }
    // not required due to the first phase, but need to entertain the compiler
    throw new IllegalArgumentException("Invalid encoded byte array");
  }

  /**
   * Returns the next long item (encoded in big-endian format via {@code writeNumIncreasing(long)})
   * from its internal encoded byte array store and removes the item from the store.
   *
   * @see #writeNumIncreasing(long)
   */
  public long readNumIncreasing() {
    if (encodedArrays.isEmpty() || (encodedArrays.get(0).length - firstArrayPosition < 1)) {
      throw new IllegalArgumentException("Invalid encoded byte array");
    }

    byte[] store = encodedArrays.get(0);
    // Decode length byte
    int len = store[firstArrayPosition];
    if ((firstArrayPosition + len + 1 > store.length) || len > 8) {
      throw new IllegalArgumentException("Invalid encoded byte array");
    }

    long result = 0;
    for (int i = 0; i < len; i++) {
      result <<= 8;
      result |= (store[firstArrayPosition + i + 1] & 0xff);
    }

    if ((store.length - firstArrayPosition - len - 1) == 0) {
      // we are done with the first array
      encodedArrays.remove(0);
      firstArrayPosition = 0;
    } else {
      firstArrayPosition = firstArrayPosition + len + 1;
    }

    return result;
  }

  /**
   * Returns the next long item (encoded in big-endian format via {@code writeNumDecreasing(long)})
   * from its internal encoded byte array store and removes the item from the store.
   *
   * @see #writeNumDecreasing(long)
   */
  public long readNumDecreasing() {
    if (encodedArrays.isEmpty() || (encodedArrays.get(0).length - firstArrayPosition < 1)) {
      throw new IllegalArgumentException("Invalid encoded byte array");
    }

    byte[] store = encodedArrays.get(0);
    // Decode length byte
    int len = ~store[firstArrayPosition] & 0xff;
    if ((firstArrayPosition + len + 1 > store.length) || len > 8) {
      throw new IllegalArgumentException("Invalid encoded byte array");
    }

    long result = 0;
    for (int i = 0; i < len; i++) {
      result <<= 8;
      result |= (~store[firstArrayPosition + i + 1] & 0xff);
    }

    if ((store.length - firstArrayPosition - len - 1) == 0) {
      // we are done with the first array
      encodedArrays.remove(0);
      firstArrayPosition = 0;
    } else {
      firstArrayPosition = firstArrayPosition + len + 1;
    }

    return result;
  }

  /**
   * Returns the next long item (encoded via {@code writeSignedNumIncreasing(long)}) from its
   * internal encoded byte array store and removes the item from the store.
   *
   * @see #writeSignedNumIncreasing(long)
   */
  public long readSignedNumIncreasing() {
    if (encodedArrays.isEmpty() || (encodedArrays.get(0).length - firstArrayPosition < 1)) {
      throw new IllegalArgumentException("Invalid encoded byte array");
    }

    byte[] store = encodedArrays.get(0);

    long xorMask = ((store[firstArrayPosition] & 0x80) == 0) ? ~0L : 0L;
    // Store first byte as an int rather than a (signed) byte -- to avoid
    // accidental byte-to-int promotion later which would extend the byte's
    // sign bit (if any).
    int firstByte = (store[firstArrayPosition] & 0xff) ^ (int) (xorMask & 0xff);

    // Now calculate and test length, and set x to raw (unmasked) result.
    int len;
    long x;
    if (firstByte != 0xff) {
      len = 7 - log2Floor(firstByte ^ 0xff);
      if (store.length - firstArrayPosition < len) {
        throw new IllegalArgumentException("Invalid encoded byte array");
      }
      x = xorMask; // Sign extend using xorMask.
      for (int i = firstArrayPosition; i < firstArrayPosition + len; i++) {
        x = (x << 8) | (store[i] & 0xff);
      }
    } else {
      len = 8;
      if (store.length - firstArrayPosition < len) {
        throw new IllegalArgumentException("Invalid encoded byte array");
      }
      int secondByte = (store[firstArrayPosition + 1] & 0xff) ^ (int) (xorMask & 0xff);
      if (secondByte >= 0x80) {
        if (secondByte < 0xc0) {
          len = 9;
        } else {
          int thirdByte = (store[firstArrayPosition + 2] & 0xff) ^ (int) (xorMask & 0xff);
          if (secondByte == 0xc0 && thirdByte < 0x80) {
            len = 10;
          } else {
            // Either len > 10 or len == 10 and #bits > 63.
            throw new IllegalArgumentException("Invalid encoded byte array");
          }
        }
        if (store.length - firstArrayPosition < len) {
          throw new IllegalArgumentException("Invalid encoded byte array");
        }
      }
      x =
          Longs.fromByteArray(
              Arrays.copyOfRange(store, firstArrayPosition + len - 8, firstArrayPosition + len));
    }

    x ^= LENGTH_TO_MASK[len]; // Remove spurious header bits.

    if (len != getSignedEncodingLength(x)) {
      throw new IllegalArgumentException("Invalid encoded byte array");
    }

    if ((store.length - firstArrayPosition - len) == 0) {
      // We are done with the first array.
      encodedArrays.remove(0);
      firstArrayPosition = 0;
    } else {
      firstArrayPosition = firstArrayPosition + len;
    }

    return x;
  }

  public long readSignedNumDecreasing() {
    return ~readSignedNumIncreasing();
  }

  /**
   * Removes INFINITY item from its internal encoded byte array store if present. Returns whether
   * INFINITY was present.
   *
   * @see #writeInfinity()
   */
  public boolean readInfinity() {
    return readInfinityInternal(INFINITY_ENCODED);
  }

  /**
   * Removes INFINITY item from its internal encoded byte array store if present. Returns whether
   * INFINITY was present.
   *
   * @see #writeInfinityDecreasing()
   */
  public boolean readInfinityDecreasing() {
    return readInfinityInternal(INFINITY_ENCODED_DECREASING);
  }

  private boolean readInfinityInternal(byte[] codes) {
    if (encodedArrays.isEmpty() || (encodedArrays.get(0).length - firstArrayPosition < 1)) {
      throw new IllegalArgumentException("Invalid encoded byte array");
    }
    byte[] store = encodedArrays.get(0);
    if (store.length - firstArrayPosition < 2) {
      return false;
    }
    if ((store[firstArrayPosition] == codes[0]) && (store[firstArrayPosition + 1] == codes[1])) {
      if ((store.length - firstArrayPosition - 2) == 0) {
        // we are done with the first array
        encodedArrays.remove(0);
        firstArrayPosition = 0;
      } else {
        firstArrayPosition = firstArrayPosition + 2;
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns the trailing byte array item from its internal encoded byte array store and removes the
   * item from the store.
   *
   * @see #writeTrailingBytes(byte[])
   */
  public byte[] readTrailingBytes() {
    // one item is contained within one byte array
    if (encodedArrays.size() != 1) {
      throw new IllegalArgumentException("Invalid encoded byte array");
    }

    byte[] store = encodedArrays.get(0);
    encodedArrays.remove(0);
    assert encodedArrays.isEmpty();
    return Arrays.copyOfRange(store, firstArrayPosition, store.length);
  }

  /**
   * Returns the encoded bytes that represents the current state of the OrderedCode.
   *
   * <p><b> NOTE: This method returns OrederedCode's internal array (not a copy) for better
   * performance. Therefore the returned array should not be modified.</b>
   */
  public byte[] getEncodedBytes() {
    if (encodedArrays.isEmpty()) {
      return new byte[0];
    }
    if ((encodedArrays.size() == 1) && (firstArrayPosition == 0)) {
      return encodedArrays.get(0);
    }

    int totalLength = 0;

    for (int i = 0; i < encodedArrays.size(); i++) {
      byte[] bytes = encodedArrays.get(i);
      if (i == 0) {
        totalLength += bytes.length - firstArrayPosition;
      } else {
        totalLength += bytes.length;
      }
    }

    byte[] encodedBytes = new byte[totalLength];
    int destPos = 0;
    for (int i = 0; i < encodedArrays.size(); i++) {
      byte[] bytes = encodedArrays.get(i);
      if (i == 0) {
        System.arraycopy(
            bytes, firstArrayPosition, encodedBytes, destPos, bytes.length - firstArrayPosition);
        destPos += bytes.length - firstArrayPosition;
      } else {
        System.arraycopy(bytes, 0, encodedBytes, destPos, bytes.length);
        destPos += bytes.length;
      }
    }

    // replace the store with merged array, so that repeated calls
    // don't need to merge. The reads can handle both the versions.
    encodedArrays.clear();
    encodedArrays.add(encodedBytes);
    firstArrayPosition = 0;

    return encodedBytes;
  }

  /**
   * Returns true if it has more encoded bytes that haven't been read, false otherwise. Return value
   * of true doesn't imply anything about validity of remaining data.
   *
   * @return true if it has more encoded bytes that haven't been read, false otherwise.
   */
  public boolean hasRemainingEncodedBytes() {
    // We delete an array after fully consuming it.
    return encodedArrays.size() != 0;
  }
}
