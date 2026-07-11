package io.github.hyshmily.zeta.demo.controller;

import io.github.hyshmily.zeta.demo.service.InterceptTestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/intercept")
public class InterceptTestController {

  @Autowired
  private InterceptTestService service;

  @GetMapping("/force/{key}")
  public String force(@PathVariable String key) {
    return service.forceIntercept(key);
  }

  @GetMapping("/hot/{key}")
  public String hot(@PathVariable String key) {
    return service.hotIntercept(key);
  }

  @GetMapping("/qps/{key}")
  public String qps(@PathVariable String key) {
    return service.qpsIntercept(key);
  }
}
