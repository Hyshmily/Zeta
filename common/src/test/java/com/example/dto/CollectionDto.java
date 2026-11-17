package com.example.dto;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Test payload whose collection fields use the JDK immutable implementations. */
public record CollectionDto(List<String> roles, Set<String> tags, Map<String, Integer> counts, List<String> aliases) {}
