package com.ivyis.di.trans.steps.mongodb;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import com.mongodb.DBObject;

/**
 * This class contains the methods to set and retrieve the status of the step data.
 * 
 * @author <a href="mailto:joel.latino@ivy-is.co.uk">Joel Latino</a>
 * @since 1.0.0
 */
public class MongoDBInsertData extends BaseStepData implements
    StepDataInterface {

  public MongoClientWrapper clientWrapper;
  public RowMetaInterface outputRowMeta;
  public DBObject[] records;
  public String hostname;
  public int port;
  public String databaseName;
  public String collectionName;

}
