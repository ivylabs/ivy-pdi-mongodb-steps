package com.ivyis.di.trans.steps.mongodb;

import org.pentaho.di.trans.step.BaseStepMeta;

/**
 * This abstract class is responsible for implementing functionality regarding step meta. All Kettle
 * steps have an extension of this where private fields have been added with public accessors.
 * 
 * @author <a href="mailto:joel.latino@ivy-is.co.uk">Joel Latino</a>
 * @since 1.0.0
 */
public abstract class MongoDBMeta extends BaseStepMeta {

  /** Hostname for database connection. */
  protected String hostname;

  /** Port for database connection. */
  protected String port;

  /** Username for database connection. */
  protected String username;

  /** Password for database connection. */
  protected String password;

  /** Database name connection. */
  protected String databaseName;

  /** Collection name connection. */
  protected String collectionName;

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getPort() {
    return port;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
  }
}
