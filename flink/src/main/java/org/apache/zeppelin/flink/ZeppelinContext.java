/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.flink;

import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.display.AngularObject;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.AngularObjectWatcher;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.display.Input.ParamOption;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterContextRunner;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.resource.Resource;
import org.apache.zeppelin.resource.ResourcePool;
import org.apache.zeppelin.resource.ResourceSet;
import scala.Tuple2;
import scala.Unit;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static scala.collection.JavaConversions.*;

/**
 * Spark context for zeppelin.
 */
public class ZeppelinContext {
  private InterpreterContext interpreterContext;
  private int maxResult;

  public ZeppelinContext(InterpreterContext interpreterContext,
                         int maxResult) {

    this.interpreterContext = interpreterContext;
    this.maxResult = maxResult;
  }

  private GUI gui;

  @ZeppelinApi
  public Object input(String name) {
    return input(name, "");
  }

  @ZeppelinApi
  public Object input(String name, Object defaultValue) {
    return gui.input(name, defaultValue);
  }

  @ZeppelinApi
  public Object select(String name, scala.collection.Iterable<Tuple2<Object, String>> options) {
    return select(name, "", options);
  }

  @ZeppelinApi
  public Object select(String name, Object defaultValue,
      scala.collection.Iterable<Tuple2<Object, String>> options) {
    return gui.select(name, defaultValue, tuplesToParamOptions(options));
  }

  @ZeppelinApi
  public scala.collection.Iterable<Object> checkbox(String name,
      scala.collection.Iterable<Tuple2<Object, String>> options) {
    List<Object> allChecked = new LinkedList<Object>();
    for (Tuple2<Object, String> option : asJavaIterable(options)) {
      allChecked.add(option._1());
    }
    return checkbox(name, collectionAsScalaIterable(allChecked), options);
  }

  @ZeppelinApi
  public scala.collection.Iterable<Object> checkbox(String name,
      scala.collection.Iterable<Object> defaultChecked,
      scala.collection.Iterable<Tuple2<Object, String>> options) {
    return collectionAsScalaIterable(gui.checkbox(name, asJavaCollection(defaultChecked),
      tuplesToParamOptions(options)));
  }

  private ParamOption[] tuplesToParamOptions(
      scala.collection.Iterable<Tuple2<Object, String>> options) {
    int n = options.size();
    ParamOption[] paramOptions = new ParamOption[n];
    Iterator<Tuple2<Object, String>> it = asJavaIterable(options).iterator();

    int i = 0;
    while (it.hasNext()) {
      Tuple2<Object, String> valueAndDisplayValue = it.next();
      paramOptions[i++] = new ParamOption(valueAndDisplayValue._1(), valueAndDisplayValue._2());
    }

    return paramOptions;
  }

  public void setGui(GUI o) {
    this.gui = o;
  }

  private void restartInterpreter() {
  }

  public InterpreterContext getInterpreterContext() {
    return interpreterContext;
  }

  public void setInterpreterContext(InterpreterContext interpreterContext) {
    this.interpreterContext = interpreterContext;
  }

  public void setMaxResult(int maxResult) {
    this.maxResult = maxResult;
  }

  /**
   * show DataFrame or SchemaRDD
   * @param o DataFrame or SchemaRDD object
   */
  @ZeppelinApi
  public void show(Object o) {
    show(o, maxResult);
  }

  /**
   * show DataFrame or SchemaRDD
   * @param o DataFrame or SchemaRDD object
   * @param maxResult maximum number of rows to display
   */

  @ZeppelinApi
  public void show(Object o, int maxResult) {

    try {
      interpreterContext.out.write(o.toString());
    } catch (IOException e) {
      throw new InterpreterException(e);
    }
  }



  /**
   * Run paragraph by id
   * @param id
   */
  @ZeppelinApi
  public void run(String id) {
    run(id, interpreterContext);
  }

  /**
   * Run paragraph by id
   * @param id
   * @param context
   */
  @ZeppelinApi
  public void run(String id, InterpreterContext context) {
    if (id.equals(context.getParagraphId())) {
      throw new InterpreterException("Can not run current Paragraph");
    }

    for (InterpreterContextRunner r : context.getRunners()) {
      if (id.equals(r.getParagraphId())) {
        r.run();
        return;
      }
    }

    throw new InterpreterException("Paragraph " + id + " not found");
  }

  /**
   * Run paragraph at idx
   * @param idx
   */
  @ZeppelinApi
  public void run(int idx) {
    run(idx, interpreterContext);
  }

  /**
   * Run paragraph at index
   * @param idx index starting from 0
   * @param context interpreter context
   */
  public void run(int idx, InterpreterContext context) {
    if (idx >= context.getRunners().size()) {
      throw new InterpreterException("Index out of bound");
    }

    InterpreterContextRunner runner = context.getRunners().get(idx);
    if (runner.getParagraphId().equals(context.getParagraphId())) {
      throw new InterpreterException("Can not run current Paragraph");
    }

    runner.run();
  }

  @ZeppelinApi
  public void run(List<Object> paragraphIdOrIdx) {
    run(paragraphIdOrIdx, interpreterContext);
  }

  /**
   * Run paragraphs
   * @param paragraphIdOrIdx list of paragraph id or idx
   */
  @ZeppelinApi
  public void run(List<Object> paragraphIdOrIdx, InterpreterContext context) {
    for (Object idOrIdx : paragraphIdOrIdx) {
      if (idOrIdx instanceof String) {
        String id = (String) idOrIdx;
        run(id, context);
      } else if (idOrIdx instanceof Integer) {
        Integer idx = (Integer) idOrIdx;
        run(idx, context);
      } else {
        throw new InterpreterException("Paragraph " + idOrIdx + " not found");
      }
    }
  }

  @ZeppelinApi
  public void runAll() {
    runAll(interpreterContext);
  }

  /**
   * Run all paragraphs. except this.
   */
  @ZeppelinApi
  public void runAll(InterpreterContext context) {
    for (InterpreterContextRunner r : context.getRunners()) {
      if (r.getParagraphId().equals(context.getParagraphId())) {
        // skip itself
        continue;
      }
      r.run();
    }
  }

  @ZeppelinApi
  public List<String> listParagraphs() {
    List<String> paragraphs = new LinkedList<String>();

    for (InterpreterContextRunner r : interpreterContext.getRunners()) {
      paragraphs.add(r.getParagraphId());
    }

    return paragraphs;
  }


  private AngularObject getAngularObject(String name, InterpreterContext interpreterContext) {
    AngularObjectRegistry registry = interpreterContext.getAngularObjectRegistry();
    String noteId = interpreterContext.getNoteId();
    // try get local object
    AngularObject paragraphAo = registry.get(name, noteId, interpreterContext.getParagraphId());
    AngularObject noteAo = registry.get(name, noteId, null);

    AngularObject ao = paragraphAo != null ? paragraphAo : noteAo;

    if (ao == null) {
      // then global object
      ao = registry.get(name, null, null);
    }
    return ao;
  }


  /**
   * Get angular object. Look up notebook scope first and then global scope
   * @param name variable name
   * @return value
   */
  @ZeppelinApi
  public Object angular(String name) {
    AngularObject ao = getAngularObject(name, interpreterContext);
    if (ao == null) {
      return null;
    } else {
      return ao.get();
    }
  }

  /**
   * Get angular object. Look up global scope
   * @param name variable name
   * @return value
   */
  @Deprecated
  public Object angularGlobal(String name) {
    AngularObjectRegistry registry = interpreterContext.getAngularObjectRegistry();
    AngularObject ao = registry.get(name, null, null);
    if (ao == null) {
      return null;
    } else {
      return ao.get();
    }
  }

  /**
   * Create angular variable in notebook scope and bind with front end Angular display system.
   * If variable exists, it'll be overwritten.
   * @param name name of the variable
   * @param o value
   */
  @ZeppelinApi
  public void angularBindNotebook(String name, Object o, String paragraphid) {
    angularBind(name, o, interpreterContext.getNoteId(), paragraphid);
  }

  /**
   * Create angular variable in notebook scope and bind with front end Angular display system.
   * If variable exists, it'll be overwritten.
   * @param name name of the variable
   * @param o value
   */
  @ZeppelinApi
  public void angularBindParagraph(String name, Object o, String paragraphid) {
    angularBind(name, o, interpreterContext.getNoteId(), paragraphid);
  }

  /**
   * Create angular variable in global scope and bind with front end Angular display system.
   * If variable exists, it'll be overwritten.
   * @param name name of the variable
   * @param o value
   */
  @Deprecated
  public void angularBindGlobal(String name, Object o) {
    angularBind(name, o, null, null, null);
  }

  /**
   * Create angular variable in local scope and bind with front end Angular display system.
   * If variable exists, value will be overwritten and watcher will be added.
   * @param name name of variable
   * @param o value
   * @param watcher watcher of the variable
   */
  @ZeppelinApi
  public void angularBind(String name, Object o,  String paragraphid, AngularObjectWatcher watcher) {
    angularBind(name, o, interpreterContext.getNoteId(), paragraphid, watcher);
  }

  /**
   * Create angular variable in global scope and bind with front end Angular display system.
   * If variable exists, value will be overwritten and watcher will be added.
   * @param name name of variable
   * @param o value
   * @param watcher watcher of the variable
   */
  @Deprecated
  public void angularBindGlobal(String name, Object o, AngularObjectWatcher watcher) {
    angularBind(name, o, null, watcher);
  }

  /**
   * Add watcher into angular variable (local scope)
   * @param name name of the variable
   * @param watcher watcher
   */
  @ZeppelinApi
  public void angularWatchParagraph(String name, String paragraphid, AngularObjectWatcher watcher) {
    angularWatch(name, interpreterContext.getNoteId(), paragraphid, watcher);
  }

  /**
   * Add watcher into angular variable (local scope)
   * @param name name of the variable
   * @param watcher watcher
   */
  @ZeppelinApi
  public void angularWatch(String name, AngularObjectWatcher watcher) {
    angularWatch(name, interpreterContext.getNoteId(), null, watcher);
  }

  /**
   * Add watcher into angular variable (global scope)
   * @param name name of the variable
   * @param watcher watcher
   */
  @Deprecated
  public void angularWatchGlobal(String name, AngularObjectWatcher watcher) {
    angularWatch(name, null, null, watcher);
  }

  @ZeppelinApi
  public void angularWatch(String name,
      final scala.Function2<Object, Object, Unit> func) {
    angularWatch(name, interpreterContext.getNoteId(), func);
  }

  @Deprecated
  public void angularWatchGlobal(String name,
      final scala.Function2<Object, Object, Unit> func) {
    angularWatch(name, null, func);
  }

  @ZeppelinApi
  public void angularWatch(
      String name,
      final scala.Function3<Object, Object, InterpreterContext, Unit> func) {
    angularWatch(name, interpreterContext.getNoteId(), func);
  }

  @Deprecated
  public void angularWatchGlobal(
      String name,
      final scala.Function3<Object, Object, InterpreterContext, Unit> func) {
    angularWatch(name, null, func);
  }


  /**
   * Remove watcher from angular variable (local)
   * @param name
   * @param watcher
   */
  @ZeppelinApi
  public void angularUnwatch(String name, AngularObjectWatcher watcher) {
    angularUnwatch(name, interpreterContext.getNoteId(), watcher);
  }

  /**
   * Remove watcher from angular variable (global)
   * @param name
   * @param watcher
   */
  @Deprecated
  public void angularUnwatchGlobal(String name, AngularObjectWatcher watcher) {
    angularUnwatch(name, null, watcher);
  }


  /**
   * Remove all watchers for the angular variable (local)
   * @param name
   */
  @ZeppelinApi
  public void angularUnwatch(String name) {
    angularUnwatch(name, interpreterContext.getNoteId());
  }

  /**
   * Remove all watchers for the angular variable (global)
   * @param name
   */
  @Deprecated
  public void angularUnwatchGlobal(String name) {
    angularUnwatch(name, (String) null);
  }

  /**
   * Remove angular variable and all the watchers.
   * @param name
   */
  @ZeppelinApi
  public void angularUnbindParagraph(String name,String paragraphid) {
    String noteId = interpreterContext.getNoteId();
    angularUnbind(name, noteId, paragraphid);
  }

  /**
   * Remove angular variable and all the watchers.
   * @param name
   */
  @ZeppelinApi
  public void angularUnbind(String name) {
    String noteId = interpreterContext.getNoteId();
    angularUnbind(name, noteId);
  }

  /**
   * Remove angular variable and all the watchers.
   * @param name
   */
  @Deprecated
  public void angularUnbindGlobal(String name) {
    angularUnbind(name, null);
  }

  /**
   * Create angular variable in notebook scope and bind with front end Angular display system.
   * If variable exists, it'll be overwritten.
   * @param name name of the variable
   * @param o value
   */
  private void angularBind(String name, Object o, String noteId, String paragraphId) {
    AngularObjectRegistry registry = interpreterContext.getAngularObjectRegistry();

    if (registry.get(name, noteId, paragraphId) == null) {
      registry.add(name, o, noteId, paragraphId);
    } else {
      registry.get(name, noteId, paragraphId).set(o);
    }
  }

  /**
   * Create angular variable in notebook scope and bind with front end Angular display
   * system.
   * If variable exists, value will be overwritten and watcher will be added.
   * @param name name of variable
   * @param o value
   * @param watcher watcher of the variable
   */
  private void angularBind(String name, Object o, String noteId, String paragraphId, AngularObjectWatcher watcher) {
    AngularObjectRegistry registry = interpreterContext.getAngularObjectRegistry();

    if (registry.get(name, noteId,  paragraphId) == null) {
      registry.add(name, o, noteId, paragraphId);
    } else {
      registry.get(name, noteId, paragraphId).set(o);
    }
    angularWatch(name, watcher);
  }

  /**
   * Add watcher into angular binding variable
   * @param name name of the variable
   * @param watcher watcher
   */
  private void angularWatch(String name, String noteId, String paragraphId, AngularObjectWatcher watcher) {
    AngularObjectRegistry registry = interpreterContext.getAngularObjectRegistry();

    if (registry.get(name, noteId, paragraphId) != null) {
      registry.get(name, noteId, paragraphId).addWatcher(watcher);
    }
  }


  private void angularWatch(String name, String noteId,
      final scala.Function2<Object, Object, Unit> func) {
    AngularObjectWatcher w = new AngularObjectWatcher(getInterpreterContext()) {
      @Override
      public void watch(Object oldObject, Object newObject,
          InterpreterContext context) {
        func.apply(newObject, newObject);
      }
    };
    angularWatch(name, noteId, null, w);
  }

  private void angularWatch(
      String name,
      String noteId,
      final scala.Function3<Object, Object, InterpreterContext, Unit> func) {
    AngularObjectWatcher w = new AngularObjectWatcher(getInterpreterContext()) {
      @Override
      public void watch(Object oldObject, Object newObject,
          InterpreterContext context) {
        func.apply(oldObject, newObject, context);
      }
    };
    angularWatch(name, noteId, null, w);
  }

  /**
   * Remove watcher
   * @param name
   * @param watcher
   */
  private void angularUnwatch(String name, String noteId, AngularObjectWatcher watcher) {
    AngularObjectRegistry registry = interpreterContext.getAngularObjectRegistry();
    if (registry.get(name, noteId, null) != null) {
      registry.get(name, noteId, null).removeWatcher(watcher);
    }
  }

  /**
   * Remove all watchers for the angular variable in a paragraph
   * @param name
   */
  private void angularUnwatch(String name, String noteId, String paragraphId) {
    AngularObjectRegistry registry = interpreterContext.getAngularObjectRegistry();
    if (registry.get(name, noteId, paragraphId) != null) {
      registry.get(name, noteId, paragraphId).clearAllWatchers();
    }
  }

  /**
   * Remove all watchers for the angular variable
   * @param name
   */
  private void angularUnwatch(String name, String noteId) {
    AngularObjectRegistry registry = interpreterContext.getAngularObjectRegistry();
    if (registry.get(name, noteId, null) != null) {
      registry.get(name, noteId, null).clearAllWatchers();
    }
  }

  /**
   * Remove angular variable and all the watchers.
   * @param name
   */
  private void angularUnbind(String name, String noteId) {
    AngularObjectRegistry registry = interpreterContext.getAngularObjectRegistry();
    registry.remove(name, noteId, null);
  }

  /**
   * Remove angular variable and all the watchers.
   * @param name
   */
  private void angularUnbind(String name, String noteId, String paragraphId) {
    AngularObjectRegistry registry = interpreterContext.getAngularObjectRegistry();
    registry.remove(name, noteId, paragraphId);
  }


  /**
   * Add object into resource pool
   * @param name
   * @param value
   */
  @ZeppelinApi
  public void put(String name, Object value) {
    ResourcePool resourcePool = interpreterContext.getResourcePool();
    resourcePool.put(name, value);
  }

  /**
   * Get object from resource pool
   * Search local process first and then the other processes
   * @param name
   * @return null if resource not found
   */
  @ZeppelinApi
  public Object get(String name) {
    ResourcePool resourcePool = interpreterContext.getResourcePool();
    Resource resource = resourcePool.get(name);
    if (resource != null) {
      return resource.get();
    } else {
      return null;
    }
  }

  /**
   * Remove object from resourcePool
   * @param name
   */
  @ZeppelinApi
  public void remove(String name) {
    ResourcePool resourcePool = interpreterContext.getResourcePool();
    resourcePool.remove(name);
  }

  /**
   * Check if resource pool has the object
   * @param name
   * @return
   */
  @ZeppelinApi
  public boolean containsKey(String name) {
    ResourcePool resourcePool = interpreterContext.getResourcePool();
    Resource resource = resourcePool.get(name);
    return resource != null;
  }

  /**
   * Get all resources
   */
  @ZeppelinApi
  public ResourceSet getAll() {
    ResourcePool resourcePool = interpreterContext.getResourcePool();
    return resourcePool.getAll();
  }

}