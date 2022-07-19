package io.confluent.services;

abstract class AbstractService {

  /** Maps the route for HTTP requests */
  public abstract void bind();
}
