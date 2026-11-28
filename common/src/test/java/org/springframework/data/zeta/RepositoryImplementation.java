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
package org.springframework.data.zeta;

/**
 * Stands in for a Spring Data repository base implementation ({@code SimpleJpaRepository} and its
 * peers): a framework-package class whose business interface is the one carrying cache
 * annotations, used to pin the advisor's class filter.
 */
public class RepositoryImplementation {

  public String findAll() {
    return "";
  }
}
