package io.github.manto.core;

/**
 * Framework-agnostic representation of a message header.
 */
public interface MantoHeader {

    /**
     * Returns the header key.
     *
     * @return the header key, never null
     */
    String key();

    /**
     * Returns the header value as a string.
     *
     * @return the header value as a string, may be null depending on encoding
     */
    String value();

    /**
     * Returns the header value as bytes.
     *
     * @return the header value as bytes, may be null
     */
    byte[] valueBytes();
}