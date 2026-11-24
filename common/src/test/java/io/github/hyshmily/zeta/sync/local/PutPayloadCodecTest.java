/*
 * Copyright 2026 Hyshmily. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.hyshmily.zeta.sync.local;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.dto.AllowedContainer;
import com.example.dto.AllowedDto;
import com.example.dto.CollectionDto;
import com.example.dtoOther.DeniedDto;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PutPayloadCodecTest {

  @Test
  void configuredPackagePrefixRequiresASegmentBoundary() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of("com.example.dto"));

    assertThat(codec.decode(codec.encode(new AllowedDto("ok")))).isEqualTo(new AllowedDto("ok"));
    assertThat(codec.decode(codec.encode(new DeniedDto("no"))))
      .as("com.example.dto must not admit the sibling com.example.dtoOther")
      .isNull();
  }

  @Test
  void anExplicitTrailingDotIsAccepted() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of("com.example.dto."));

    assertThat(codec.decode(codec.encode(new AllowedDto("ok")))).isEqualTo(new AllowedDto("ok"));
  }

  @Test
  void builtInMutableCharSequencesStillRoundTrip() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of());

    assertThat(codec.decode(codec.encode(new StringBuilder("builder")))).hasToString("builder");
    assertThat(codec.decode(codec.encode(new StringBuffer("buffer")))).hasToString("buffer");
  }

  /**
   * The allowlist admits {@code java.time}, but those values need their Jackson modules. The
   * mapper discovers the modules on the application classpath; without them encode returns null
   * and every PUT carrying a time value silently degrades to REFRESH.
   */
  @Test
  void discoveredModulesRoundTripJavaTimeValues() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of());

    Instant instant = Instant.ofEpochMilli(1_700_000_000_000L);
    assertThat(codec.decode(codec.encode(instant))).isEqualTo(instant);
    LocalDate date = LocalDate.of(2025, 1, 2);
    assertThat(codec.decode(codec.encode(date))).isEqualTo(date);
  }

  /**
   * Java 9 immutable collections are serialized under their internal runtime class. Jackson
   * maps those back to standard mutable implementations; value equality is the contract that
   * matters to a PUT receiver.
   */
  @Test
  void immutableCollectionsRoundTripByValue() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of());

    assertThat(codec.decode(codec.encode(List.of("a", "b")))).isEqualTo(List.of("a", "b"));
    assertThat(codec.decode(codec.encode(Set.of("a", "b")))).isEqualTo(Set.of("a", "b"));
    assertThat(codec.decode(codec.encode(Map.of("a", 1, "b", 2)))).isEqualTo(Map.of("a", 1, "b", 2));
  }

  /**
   * A JDK collection view or internal immutable implementation has no public constructor on
   * the receiving side; its type id is normalized to the standard mutable container so the
   * value round-trips instead of degrading the PUT to an INVALIDATE.
   */
  @Test
  void jdkCollectionViewsRoundTripAsStandardContainers() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of());

    Object arrayList = codec.decode(codec.encode(Arrays.asList("a", "b")));
    assertThat(arrayList).isEqualTo(List.of("a", "b")).isInstanceOf(ArrayList.class);

    Object singletonList = codec.decode(codec.encode(Collections.singletonList("a")));
    assertThat(singletonList).isEqualTo(List.of("a")).isInstanceOf(ArrayList.class);

    Object copies = codec.decode(codec.encode(Collections.nCopies(2, "a")));
    assertThat(copies).isEqualTo(List.of("a", "a")).isInstanceOf(ArrayList.class);

    Object checked = codec.decode(codec.encode(Collections.checkedList(new ArrayList<>(List.of("a")), String.class)));
    assertThat(checked).isEqualTo(List.of("a")).isInstanceOf(ArrayList.class);

    Object singletonSet = codec.decode(codec.encode(Collections.singleton("a")));
    assertThat(singletonSet).isEqualTo(Set.of("a")).isInstanceOf(HashSet.class);

    Object singletonMap = codec.decode(codec.encode(Collections.singletonMap("a", 1)));
    assertThat(singletonMap).isEqualTo(Map.of("a", 1)).isInstanceOf(HashMap.class);
  }

  /** The normalization also applies to immutable collection fields nested in a DTO. */
  @Test
  void dtoFieldsWithJdkImmutableCollectionsRoundTrip() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of("com.example.dto"));
    CollectionDto dto = new CollectionDto(List.of("a"), Set.of("b"), Map.of("c", 1), Arrays.asList("d", "e"));

    assertThat(codec.decode(codec.encode(dto))).isEqualTo(dto);
  }

  /** A legacy Map root is copied to a standard Map and round-trips as data. */
  @Test
  void legacyMapRootsBecomeStandardMaps() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of());

    assertThat(codec.decode(codec.encode(new Properties()))).isEqualTo(Map.of());

    Hashtable<String, String> table = new Hashtable<>();
    table.put("k", "v");
    assertThat(codec.decode(codec.encode(table))).isEqualTo(Map.of("k", "v"));
  }

  /**
   * A Map subclass outside the allowlist is data, not identity: the root is normalized to a
   * standard map and round-trips by value instead of degrading the PUT to an INVALIDATE.
   */
  @Test
  void foreignMapSubclassRootRoundTripsAsData() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of());
    ThirdPartyMap map = new ThirdPartyMap();
    map.put("k", "v");

    assertThat(codec.decode(codec.encode(map))).isEqualTo(Map.of("k", "v"));
  }

  /**
   * An array type id is judged on its component: the array form of an admitted class round-trips,
   * while the array form of a denied class is rejected. An {@code isArray()} shortcut admits both.
   */
  @Test
  void arrayTypeIdIsJudgedOnItsComponent() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of("com.example.dto"));
    String admitted = "[\"[L" + AllowedDto.class.getName() + ";\",[]]";
    String denied = "[\"[L" + DeniedDto.class.getName() + ";\",[]]";

    assertThat(codec.decode(admitted.getBytes(StandardCharsets.UTF_8))).isInstanceOf(AllowedDto[].class);
    assertThat(codec.decode(denied.getBytes(StandardCharsets.UTF_8))).isNull();
  }

  /** {@code java.lang} and {@code java.util} admit value classes only, never side-effecting ones. */
  @Test
  void sideEffectingJdkTypesAreNeverInstantiated() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of());

    assertThat(codec.decode("[\"java.lang.Thread\",{}]".getBytes(StandardCharsets.UTF_8))).isNull();
    assertThat(codec.decode("[\"java.util.Timer\",{}]".getBytes(StandardCharsets.UTF_8))).isNull();
    assertThat(codec.decode("[\"java.util.logging.FileHandler\",{}]".getBytes(StandardCharsets.UTF_8))).isNull();
    assertThat(codec.decode("[\"[Ljava.lang.ProcessBuilder;\",[]]".getBytes(StandardCharsets.UTF_8))).isNull();
  }

  /** A JDK immutable collection type id still decodes into a standard container. */
  @Test
  void jdkImmutableCollectionTypeIdsAreAdmittedByExactName() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of());

    assertThat(
      codec.decode("[\"java.util.ImmutableCollections$List12\",[\"a\"]]".getBytes(StandardCharsets.UTF_8))
    ).isEqualTo(List.of("a"));
  }

  /** A container in an allowed package is a normal allowlisted value. */
  @Test
  void aContainerInAnAllowedPackageRoundTrips() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of("com.example.dto"));
    AllowedContainer container = new AllowedContainer();
    container.put("k", "v");

    assertThat(codec.decode(codec.encode(container))).isEqualTo(Map.of("k", "v"));
  }

  /** An EnumSet/EnumMap root is copied to a standard container; the enum value keeps its type. */
  @Test
  void enumContainersRoundTripAsStandardContainers() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of());

    assertThat(codec.decode(codec.encode(EnumSet.of(java.time.DayOfWeek.MONDAY)))).isEqualTo(
      Set.of(java.time.DayOfWeek.MONDAY)
    );

    Map<java.time.DayOfWeek, String> enumMap = new EnumMap<>(java.time.DayOfWeek.class);
    enumMap.put(java.time.DayOfWeek.MONDAY, "start");
    assertThat(codec.decode(codec.encode(enumMap))).isEqualTo(Map.of("MONDAY", "start"));
  }

  /**
   * A type id naming an allowed container together with an unlisted element type must be refused:
   * the element type is not a value this codec may instantiate.
   */
  @Test
  void anAllowedContainerCannotSmuggleAnUnlistedTypeArgument() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of());
    String denied = TestPayload.class.getName();

    assertThat(
      codec.decode(("[\"java.util.ArrayList<" + denied + ">\",[{\"value\":\"x\"}]]").getBytes(StandardCharsets.UTF_8))
    ).isNull();
    String mapTypeId = "[\"java.util.HashMap<java.lang.String," + denied + ">\",{}]";
    assertThat(codec.decode(mapTypeId.getBytes(StandardCharsets.UTF_8))).isNull();
  }

  /** A primitive array's component has no allowlisted class name and must still be admitted. */
  @Test
  void primitiveArraysKeepTheirComponentType() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of());

    Object bytes = codec.decode(codec.encode(new byte[] { 1, 2, 3 }));
    assertThat(bytes).isInstanceOf(byte[].class);
    assertThat((byte[]) bytes).containsExactly(1, 2, 3);

    assertThat(codec.decode(codec.encode(new int[] { 7 }))).isInstanceOf(int[].class);
  }

  /** A root LinkedList is a standard container and keeps its runtime type instead of being copied. */
  @Test
  void aLinkedListRootKeepsItsType() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of());

    assertThat(codec.decode(codec.encode(new LinkedList<>(List.of("a"))))).isInstanceOf(LinkedList.class);
  }

  /** An array keeps its component type on the wire. */
  @Test
  void arrayTypesRoundTrip() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of());

    Object strings = codec.decode(codec.encode(new String[] { "a", "b" }));
    assertThat(strings).isInstanceOf(String[].class);
    assertThat((String[]) strings).containsExactly("a", "b");
  }

  /** The built-in allowlist admits {@code io.github.hyshmily.zeta.model.*}, not the whole framework. */
  @Test
  void frameworkPackagesOutsideModelAreDenied() {
    PutPayloadCodec codec = new PutPayloadCodec(4_096, List.of());
    TestPayload payload = new TestPayload("x");

    assertThat(codec.encode(payload)).isNotNull();
    assertThat(codec.decode(codec.encode(payload))).isNull();
  }

  /** A Map implementation on the application classpath, outside {@code java.util}. */
  private static final class ThirdPartyMap extends HashMap<String, String> {}

  /** A framework-owned class outside {@code io.github.hyshmily.zeta.model}. */
  private record TestPayload(String value) {}
}
