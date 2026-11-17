package com.example.dto;

import java.util.HashMap;

/**
 * A Map implementation inside an allowed package: the package prefix admits its name, but a
 * payload value type must still be a JDK container.
 */
public class AllowedContainer extends HashMap<String, String> {

  private static final long serialVersionUID = 1L;
}
