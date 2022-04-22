/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.flink.bigquery.unittest;

import com.google.cloud.flink.bigquery.common.UserAgentHeaderProvider;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UserAgentHeaderProviderTest {
  @Before
  public void setup() {
    UserAgentHeaderProvider userAgentHeaderProvider;
  }

  @Test
  public void getHeadersTest() {
    UserAgentHeaderProvider userAgentHeaderProvider =
        new UserAgentHeaderProvider("test agent header");
    Map<String, String> resultMap = userAgentHeaderProvider.getHeaders();
    String userAgent = resultMap.get("user-agent");
    Assert.assertNotNull(resultMap);
    Assert.assertNotNull(userAgent);
    Assert.assertTrue(resultMap instanceof Map);
    Assert.assertTrue(userAgent instanceof String);
    Assert.assertEquals(userAgent, "test agent header");
  }
}
