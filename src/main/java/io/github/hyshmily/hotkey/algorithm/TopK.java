package io.github.hyshmily.hotkey.algorithm;

import java.util.List;
import java.util.concurrent.BlockingQueue;

// 热点检测
// Hot key detection
public interface TopK {
  // 添加一次访问，返回结果（是否成为热点、是否挤出旧热点）
  // Add one access and return the result (whether it becomes hot, whether old hot key is evicted)
  AddResult add(String key, int increment);

  // 获取当前 TopK 列表（按计数降序）
  // Get the current TopK list (descending by count)
  List<Item> list();

  // 获取被挤出 TopK 的项队列，供外部异步处理
  // Get the queue of items evicted from TopK for external async processing
  BlockingQueue<Item> expelled();

  // 对所有计数进行衰减（用于老化历史数据）
  // Decay all counts (for aging historical data)
  void fading();

  // 返回数据流总数
  // Return the total number of data streams
  long total();
}
