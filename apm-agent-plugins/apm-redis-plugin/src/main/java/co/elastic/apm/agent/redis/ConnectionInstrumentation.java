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

import co.elastic.apm.agent.bci.ElasticApmInstrumentation;
import co.elastic.apm.agent.bci.VisibleForAdvice;
import co.elastic.apm.agent.impl.ElasticApmTracer;
import co.elastic.apm.agent.impl.transaction.Span;
import co.elastic.apm.agent.impl.transaction.TraceContextHolder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.NamedElement;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

import javax.annotation.Nullable;
import redis.clients.jedis.Connection;
import redis.clients.jedis.commands.ProtocolCommand;
import java.util.Collection;
import java.util.Collections;

import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.isInterface;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.nameContains;
import static net.bytebuddy.matcher.ElementMatchers.nameStartsWith;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.not;
import static net.bytebuddy.matcher.ElementMatchers.returns;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

/**
 * Matches the sendCommand method in the jedis.Connection
 * and creates a span for each command
 */
public class ConnectionInstrumentation extends ElasticApmInstrumentation {

  static final String INSTRUMENTATION_GROUP = "redis";
  public static final String DB_SPAN_TYPE = "db";
  public static final String DB_SPAN_ACTION = "query";

  /*
   * This makes sure that even when there are wrappers for the statement,
   * we only record each JDBC call once.
   */
  private static boolean isAlreadyMonitored(@Nullable TraceContextHolder<?> parent) {
    if (!(parent instanceof Span)) {
      return false;
    }
    Span parentSpan = (Span) parent;
    // a db span can't be the child of another db span
    // this means the span has already been created for this db call
    return parentSpan.getType() != null && parentSpan.getType().equals(DB_SPAN_TYPE);
  }

  @Nullable
  public static Span createRedisCommandSpan(@Nullable String command, @Nullable byte[][] args, @Nullable TraceContextHolder<?> parent) {
    if (command == null || isAlreadyMonitored(parent) || parent == null || !parent.isSampled()) {
      return null;
    }
    Span span = parent.createSpan().activate();
    span.setName(command);

    StringBuilder stmt = new StringBuilder(command);
    for (int i = 0; i < args.length; i++) {
        if (args[i] != null) {
            stmt.append(" ");
            stmt.append(new String(args[i]));
        }
    }

    // setting the type here is important
    // getting the meta data can result in another jdbc call
    // if that is traced as well -> StackOverflowError
    // to work around that, isAlreadyMonitored checks if the parent span is a db span and ignores them
    span.withType(DB_SPAN_TYPE);
    try {
      span.withSubtype("redis")
        .withAction(DB_SPAN_ACTION);
      span.getContext().getDb()
        .withStatement(stmt.toString())
        .withType("redis");
    } catch (Exception e) {

    }
    return span;
  }

  // not inlining as we can then set breakpoints into this method
  // also, we don't have class loader issues when doing so
  // another benefit of not inlining is that the advice methods are included in coverage reports
  @Nullable
  @VisibleForAdvice
  @Advice.OnMethodEnter(suppress = Throwable.class)
    public static Span onBeforeSendCommand(@Advice.This Connection c, @Advice.Argument(0) ProtocolCommand pc, @Advice.Argument(1) byte[][] args) {
    if (tracer != null) {
      final @Nullable String command = new String(pc.getRaw());

      return createRedisCommandSpan(command, args, tracer.getActive());
    }
    return null;
  }

  @VisibleForAdvice
  @Advice.OnMethodExit(suppress = Throwable.class, onThrowable = Throwable.class)
    public static void onAfterSendCommand(@Advice.Enter @Nullable Span span, @Advice.Thrown Throwable t) {
    if (span != null) {
      span.captureException(t)
        .deactivate()
        .end();
    }
  }

  @Override
  public ElementMatcher<? super NamedElement> getTypeMatcherPreFilter() {
    return nameContains("Connection");
  }

  @Override
  public ElementMatcher<? super TypeDescription> getTypeMatcher() {
    return not(isInterface())
      .and(hasSuperType(named("redis.clients.jedis.Connection")));
  }

  @Override
  public ElementMatcher<? super MethodDescription> getMethodMatcher() {
    return nameStartsWith("sendCommand")
        .and(takesArgument(0, ProtocolCommand.class))
        .and(takesArgument(1, byte [][].class))
        .and(isPublic());
  }

  @Override
  public Collection<String> getInstrumentationGroupNames() {
    return Collections.singleton(INSTRUMENTATION_GROUP);
  }

}
