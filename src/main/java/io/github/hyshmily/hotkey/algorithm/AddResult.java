package io.github.hyshmily.hotkey.algorithm;

// TopK.add() 的返回结果
// Result of TopK.add()
//
// @param expelledKey 本次操作被挤出 TopK 的 key，没有则为 null
// @param expelledKey The key evicted from TopK by this operation, or null
// @param isHotKey    当前 key 是否进入了 TopK 热点集合
// @param isHotKey    Whether the current key entered the TopK hot set
// @param currentKey  本次操作的 key
// @param currentKey  The key of this operation
public record AddResult(String expelledKey, boolean isHotKey, String currentKey) {}
