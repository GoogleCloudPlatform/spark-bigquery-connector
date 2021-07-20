package com.google.cloud.spark.bigquery.spark3;

import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.function.Function;

public class SerializableAbstractFunction1<T, U> extends AbstractFunction1<T, U>
    implements Serializable {
  private Function<T, U> func;

  SerializableAbstractFunction1(Function<T, U> func) {
    this.func = func;
  }

  @Override
  public U apply(T obj) {
    return func.apply(obj);
  }
}
