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

  /** The servers list. */
  protected String servers;

  /** Username for database connection. */
  protected String username;

  /** Password for database connection. */
  protected String password;

  /** authentication database */
  protected String authDb;

  /** authentication mechanism */
  protected String authMechanism;

  /** Database name connection. */
  protected String databaseName;

  /** Collection name connection. */
  protected String collectionName;


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

  public String getAuthDb() {
    return authDb;
  }

  public void setAuthDb( String authDb ) {
    this.authDb = authDb;
  }

  public String getAuthMechanism() {
    return authMechanism;
  }

  public void setAuthMechanism( String authMechanism ) {
    this.authMechanism = authMechanism;
  }

  public String getServers() {
    return servers;
  }

  public void setServers( String servers ) {
    this.servers = servers;
  }
}
