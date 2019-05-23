/*-
 * #%L
 * Elastic APM Java agent
 * %%
 * Copyright (C) 2018 - 2019 Elastic and contributors
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package co.elastic.apm.agent.redis;

import co.elastic.apm.agent.AbstractInstrumentationTest;
import co.elastic.apm.agent.impl.transaction.Db;
import co.elastic.apm.agent.impl.transaction.Span;
import co.elastic.apm.agent.impl.transaction.TraceContext;
import co.elastic.apm.agent.impl.transaction.Transaction;

import com.github.zxl0714.redismock.RedisServer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Connection;
import redis.clients.jedis.commands.ProtocolCommand;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Uses plain connections without a connection pool
 */
public class RedisInstrumentationTest extends AbstractInstrumentationTest {

  private final String expectedDbVendor = "redis";
  private Jedis jedis;
  private static RedisServer server = null;
  @Nullable
  private final Transaction transaction;

  public RedisInstrumentationTest() {
    transaction = tracer.startTransaction(TraceContext.asRoot(), null, null).activate();
    transaction.setName("transaction");
    transaction.withType("request");
    transaction.withResult("success");
  }

  @Before
  public void setUp() throws Exception {
      this.server = RedisServer.newRedisServer();  // bind to a random port
      this.server.start();
  }

  @After
  public void tearDown() {
    server.stop();
    server = null;
  }

  // execute in a single test because creating a new connection is expensive,
  // as it spins up another docker container
  @Test
  public void test() {
    System.out.println("test() start");
    Jedis j = new Jedis(server.getHost(), server.getBindPort());
    System.out.println("test() about to jedis.set");
    j.set("foo", "bar");
    System.out.println("test() about to assert");
    assertSpanRecorded(j);
    System.out.println("test() about to deactivate trans");
    transaction.deactivate().end();
    System.out.println("test() end");
  }

  private void assertSpanRecorded(Jedis j) {
    assertThat(reporter.getSpans()).hasSize(1);
    assertThat(j.get("foo")).isEqualTo("bar");
    assertThat(reporter.getSpans()).hasSize(2); // the GET we just did
    Span redisSpan = reporter.getFirstSpan();
    assertThat(redisSpan.getName().toString()).isEqualTo("SET");
    assertThat(redisSpan.getType()).isEqualTo("db");
    assertThat(redisSpan.getSubtype()).isEqualTo(expectedDbVendor);
    assertThat(redisSpan.getAction()).isEqualTo("query");
    Db db = redisSpan.getContext().getDb();
    assertThat(db.getStatement()).isEqualTo("SET foo bar");
    assertThat(db.getType()).isEqualToIgnoringCase("redis");
    reporter.reset();
  }
}
