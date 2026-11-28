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
package io.github.hyshmily.zeta.annotation.support;

import io.github.hyshmily.zeta.cache.CacheKeysPolicy;
import io.github.hyshmily.zeta.policy.KeyFormat;
import io.github.hyshmily.zeta.util.Internal;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jspecify.annotations.Nullable;

/**
 * The plan-time registry of each namespace's physical key format.
 *
 * <p>It is written while beans are proxied and read by the facade's clear/view operations. It
 * deliberately does not own loaders or triggers: those are the warm registry's frozen snapshot.
 *
 * <p>A namespace has exactly one format. A declaration that names a format pins it; an
 * {@code INHERIT} declaration follows a format a programmatic registration already configured and
 * otherwise pins the global default, so two annotations can never disagree because of scan order.
 * A keyless declaration — a table-wide clear — names no format at all and leaves the namespace
 * unpinned: it only guarantees that {@link #resolve} answers instead of failing.
 *
 * <p>A scan stages its declarations in a {@link Staging} area and publishes them in one
 * synchronized commit, so a scan that fails on a later method leaves no namespace entry behind.
 */
@Internal
public final class NamespaceKeys {

  private final KeyFormat defaultFormat;
  private final ConcurrentMap<String, Entry> byName = new ConcurrentHashMap<>();

  /**
   * Creates the registry.
   *
   * @param defaultFormat the global default; {@code null} means {@link KeyFormat#TYPED}
   */
  public NamespaceKeys(@Nullable KeyFormat defaultFormat) {
    this.defaultFormat = defaultFormat == null ? KeyFormat.TYPED : defaultFormat;
  }

  /**
   * Register a namespace that has no annotation declaration, for the programmatic facade.
   *
   * <p>An explicit format pins the namespace; a conflicting declaration fails here. Omitting the
   * format adopts the annotation's format when one exists, and otherwise pins the global default.
   *
   * @param namespace the logical cache name
   * @param format    the declared format, or {@code null} to inherit
   * @return the namespace's encoder
   * @throws IllegalStateException when an explicit format conflicts with a registration
   */
  public KeyEncoding registerProgrammatic(String namespace, @Nullable KeyFormat format) {
    validateName(namespace);
    synchronized (this) {
      Entry next = resolveProgrammatic(namespace, format, byName.get(namespace));
      byName.put(namespace, next);
      return next.encoding();
    }
  }

  /**
   * Opens a staging area for one scan. Staged declarations are invisible to {@link #resolve} and to
   * other scans until {@link Staging#commit()}.
   *
   * @return a new, single-threaded staging area
   */
  public Staging staging() {
    return new Staging();
  }

  /**
   * The namespace's encoder.
   *
   * @param namespace the logical cache name
   * @return the encoder
   * @throws IllegalStateException when no declaration registered the namespace
   */
  public KeyEncoding resolve(String namespace) {
    Entry entry = byName.get(namespace);
    if (entry == null) {
      throw new IllegalStateException(
        "unknown namespace '" + namespace + "'; declare it on an annotated method or register it before use"
      );
    }
    return entry.encoding();
  }

  /**
   * The physical prefix, including the format delimiter.
   *
   * @param namespace the logical cache name
   * @return {@code namespace + delimiter}
   */
  public String physicalPrefixOf(String namespace) {
    return resolve(namespace).physicalPrefix();
  }

  /**
   * The registered namespaces, for diagnostics.
   *
   * @return the logical name of every registered namespace and its resolved format
   */
  public Map<String, KeyFormat> formats() {
    Map<String, KeyFormat> formats = new LinkedHashMap<>(byName.size());
    byName.forEach((name, entry) -> formats.put(name, entry.encoding().format()));
    return Map.copyOf(formats);
  }

  /**
   * Validate one logical namespace name. Both delimiters are reserved so a PLAIN prefix and a
   * TYPED prefix can never match the same key.
   *
   * @param namespace the name to validate
   */
  public static void validateName(String namespace) {
    if (namespace == null || namespace.isBlank()) {
      throw new IllegalArgumentException("namespace must not be blank");
    }
    if (CacheKeysPolicy.containsDelimiter(namespace)) {
      throw new IllegalArgumentException("namespace '" + namespace + "' must not contain ':' or U+0000");
    }
  }

  /**
   * Resolves one annotated declaration against the registry state it observes.
   *
   * @param namespace the logical cache name
   * @param option    the declared format option, or {@code null} for inherit
   * @param method    the annotated method, named in conflict errors
   * @param pin       whether the declaration names a format; a keyless clear passes {@code false}
   * @param existing  the current entry, or {@code null}
   * @return the entry this declaration resolves to
   */
  private Entry resolveAnnotated(
    String namespace,
    KeyFormat.@Nullable Option option,
    Method method,
    boolean pin,
    @Nullable Entry existing
  ) {
    KeyFormat explicit = option == null ? null : option.toFormat();
    if (!pin && explicit == null) {
      return existing != null ? existing : new Entry(new KeyEncoding(namespace, defaultFormat), Origin.UNPINNED);
    }
    KeyFormat target = explicit != null
      ? explicit
      : existing != null && existing.origin() == Origin.CONFIGURED
        ? existing.encoding().format()
        : defaultFormat;
    if (existing == null || existing.origin() == Origin.UNPINNED) {
      KeyEncoding encoding = existing != null && existing.encoding().format() == target
        ? existing.encoding()
        : new KeyEncoding(namespace, target);
      return new Entry(encoding, Origin.ANNOTATED);
    }
    if (existing.encoding().format() != target) {
      throw conflict(namespace, existing.encoding().format(), target, method);
    }
    return existing;
  }

  /**
   * Resolves one programmatic registration against the registry state it observes.
   *
   * @param namespace the logical cache name
   * @param requested the declared format, or {@code null} to inherit
   * @param existing  the current entry, or {@code null}
   * @return the entry this registration resolves to
   */
  private Entry resolveProgrammatic(String namespace, @Nullable KeyFormat requested, @Nullable Entry existing) {
    if (requested == null) {
      if (existing == null) {
        return new Entry(new KeyEncoding(namespace, defaultFormat), Origin.ANNOTATED);
      }
      return existing.origin() == Origin.UNPINNED ? new Entry(existing.encoding(), Origin.ANNOTATED) : existing;
    }
    if (existing == null || existing.origin() == Origin.UNPINNED) {
      KeyEncoding encoding = existing != null && existing.encoding().format() == requested
        ? existing.encoding()
        : new KeyEncoding(namespace, requested);
      return new Entry(encoding, Origin.CONFIGURED);
    }
    if (existing.encoding().format() != requested) {
      throw conflict(namespace, existing.encoding().format(), requested, null);
    }
    return existing.origin() == Origin.CONFIGURED ? existing : new Entry(existing.encoding(), Origin.CONFIGURED);
  }

  private static IllegalStateException conflict(
    String namespace,
    KeyFormat existing,
    KeyFormat requested,
    @Nullable Method method
  ) {
    String suffix = method == null ? "" : " on " + method;
    return new IllegalStateException(
      "conflicting key format for namespace '" + namespace + "': " + existing + " vs " + requested + suffix
    );
  }

  private static IllegalStateException drifted(
    String namespace,
    KeyFormat committed,
    KeyFormat staged,
    Method method
  ) {
    return new IllegalStateException(
      "namespace '" +
        namespace +
        "' was registered as " +
        committed +
        " while this scan resolved it as " +
        staged +
        " on " +
        method +
        "; a concurrent declaration cannot change a namespace's key format mid-scan"
    );
  }

  /**
   * How a namespace entry came to exist. An unpinned entry carries the default format but no
   * declaration, so any later declaration may replace that format without a conflict.
   */
  private enum Origin {
    /** A keyless clear only required the namespace to exist. */
    UNPINNED,
    /** An annotation pinned the format. */
    ANNOTATED,
    /** A programmatic registration pinned the format. */
    CONFIGURED,
  }

  private record Entry(KeyEncoding encoding, Origin origin) {}

  private record Pending(Entry entry, KeyFormat.@Nullable Option option, Method method, boolean pinned) {}

  /**
   * One scan's provisional namespace declarations, committed in a single step.
   *
   * <p>The stage area is single-threaded: one class scan builds it, publishes its other plan state,
   * and commits it before that scan returns.
   */
  public final class Staging {

    private final Map<String, Pending> staged = new LinkedHashMap<>(4);

    private Staging() {}

    /**
     * Stages one annotated declaration and returns the encoder its plan inlines.
     *
     * <p>A keyless clear ({@code pin = false}) never replaces a format another declaration of the
     * same scan already pinned, while a key-bearing declaration always supersedes it.
     *
     * @param namespace the logical cache name
     * @param option    the declared format option, or {@code null} for inherit
     * @param method    the annotated method, named in conflict errors
     * @param pin       whether the declaration names a format
     * @return the encoder this declaration resolves to
     */
    public KeyEncoding stage(String namespace, KeyFormat.@Nullable Option option, Method method, boolean pin) {
      validateName(namespace);
      Pending previous = staged.get(namespace);
      Entry base = previous != null ? previous.entry() : byName.get(namespace);
      Entry next = resolveAnnotated(namespace, option, method, pin, base);
      boolean pinned = pin || (option != null && option != KeyFormat.Option.INHERIT);
      if (previous == null || (!previous.pinned() && pinned)) {
        staged.put(namespace, new Pending(next, option, method, pinned));
      }
      return next.encoding();
    }

    /**
     * Publishes every staged declaration under one lock.
     *
     * <p>Each entry is resolved again against the committed registry, so a registration that landed
     * between this scan's first stage and its commit cannot be overwritten by a stale answer. A
     * pinned entry whose format moved is a startup failure rather than a silent re-encode.
     *
     * @throws IllegalStateException when a concurrent registration changed a pinned namespace's
     *                               key format
     */
    public void commit() {
      if (staged.isEmpty()) {
        return;
      }
      synchronized (NamespaceKeys.this) {
        Map<String, Entry> resolved = new LinkedHashMap<>(staged.size());
        for (Map.Entry<String, Pending> item : staged.entrySet()) {
          String namespace = item.getKey();
          Pending pending = item.getValue();
          Entry next = resolveAnnotated(
            namespace,
            pending.option(),
            pending.method(),
            pending.pinned(),
            byName.get(namespace)
          );

          if (pending.pinned() && next.encoding().format() != pending.entry().encoding().format()) {
            throw drifted(
              namespace,
              next.encoding().format(),
              pending.entry().encoding().format(),
              pending.method()
            );
          }
          resolved.put(namespace, next);
        }
        byName.putAll(resolved);
      }
    }
  }
}
