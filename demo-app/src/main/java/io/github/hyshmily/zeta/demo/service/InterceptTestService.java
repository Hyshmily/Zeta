package io.github.hyshmily.zeta.demo.service;

import io.github.hyshmily.zeta.annotation.Intercept;
import io.github.hyshmily.zeta.annotation.InterceptType;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

@Service
public class InterceptTestService {

  @Cacheable(cacheNames = "intercept", key = "#p0")
  @Intercept(type = InterceptType.FORCE, fallback = "'force-fallback-' + #p0")
  public String forceIntercept(String key) {
    return "real-" + key;
  }

  @Cacheable(cacheNames = "intercept", key = "#p0")
  @Intercept(type = InterceptType.IS_LOCAL_HOT, fallback = "'hot-fallback-' + #p0")
  public String hotIntercept(String key) {
    return "hot-real-" + key;
  }

  @Cacheable(cacheNames = "intercept", key = "#p0")
  @Intercept(type = InterceptType.QPS, qps = 100, fallback = "'qps-fallback-' + #p0")
  public String qpsIntercept(String key) {
    return "qps-real-" + key;
  }
}
