package com.example.dtoOther;

/**
 * Test payload in a sibling package: a raw {@code com.example.dto} prefix would admit it, a
 * segment-boundary prefix must not.
 */
public record DeniedDto(String value) {}
