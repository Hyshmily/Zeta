package io.github.hyshmily.hotkey;

import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyCache;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class HotKey {

    private final HotKeyCache hotKeyCache;
    private final TopK topKAlgorithm;

    public boolean isHotKey(String cacheKey) {
        return hotKeyCache.isHotKey(cacheKey);
    }

    public List<Item> returnHotKeys() {
        return topKAlgorithm.list();
    }

    public BlockingQueue<Item> returnExpelledHotKeys() {
        return topKAlgorithm.expelled();
    }

    public long  returnTotalDataStreams(){
        return topKAlgorithm.total();
    }

    public <T> Optional<T> peek(String cacheKey) {
        return hotKeyCache.peek(cacheKey);
    }

    public <T> Optional<T> get(String cacheKey, Supplier<T> redisReader) {
        return hotKeyCache.get(cacheKey, redisReader);
    }

    public <T> Optional<T> getRelaxed(String cacheKey, Supplier<T> redisReader) {
        return hotKeyCache.getWithStale(cacheKey, redisReader);
    }

    public void invalidate(String cacheKey) {
        hotKeyCache.invalidateOrUpdate(cacheKey);
    }

    public void invalidateAll(String... cacheKeys) {
        invalidateAll(Arrays.asList(cacheKeys));
    }

    public void invalidateAll(Collection<String> cacheKeys) {
        hotKeyCache.invalidateAll(cacheKeys);
    }

    public void putThrough(String cacheKey, Object value, Runnable redisWriter) {
        hotKeyCache.putThrough(cacheKey, value, redisWriter);
    }

    public void putBeforeInvalidate(String cacheKey, Runnable redisMutation) {
        hotKeyCache.putInvalidate(cacheKey, redisMutation);
    }
}