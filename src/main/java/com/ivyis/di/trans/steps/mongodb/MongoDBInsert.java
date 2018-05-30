package com.ivyis.di.trans.steps.mongodb;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

/**
 * This class is responsible to processing the data rows.
 * 
 * @author <a href="mailto:joel.latino@ivy-is.co.uk">Joel Latino</a>
 * @since 1.0.0
 */
public class MongoDBInsert extends BaseStep implements StepInterface {
  private static final Class<?> PKG = MongoDBInsert.class;

  private MongoDBInsertMeta meta;
  private MongoDBInsertData data;

  public MongoDBInsert(StepMeta s, StepDataInterface stepDataInterface,
      int c, TransMeta t, Trans dis) {
    super(s, stepDataInterface, c, t, dis);
  }

  /**
   * {@inheritDoc}
   * 
   * @throws KettleException
   */
  @Override
  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
      throws KettleException {
    try {
      final Object[] r = getRow();
      if (r == null) {
        data.clientWrapper.dispose();
        setOutputDone();
        return false;
      }

      try {
        if (first) {
          data.outputRowMeta = getInputRowMeta().clone();
          // Get output field types
          meta.getFields(data.outputRowMeta, getStepname(), null,
              null, this);
          first = false;
        }

        final Object[] outputRowData = r;

        // insertData(array);

        putRow(data.outputRowMeta, outputRowData);

      } catch (Exception e) {
        putError(data.outputRowMeta, r, 1, "Error on insert data",
            "json", "generic error");
        return false;
      }
    } catch (Exception e) {
      throw new KettleException(e);
    }
    return true;

  }

  private void insertData(DBObject[] array) throws KettleException {
    final WriteResult cur = data.clientWrapper.getCollection(
        data.databaseName, data.collectionName).insert(data.records,
        WriteConcern.NORMAL);
    data.records = null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    if (super.init(smi, sdi)) {
      meta = (MongoDBInsertMeta) smi;
      data = (MongoDBInsertData) sdi;

      data.databaseName = environmentSubstitute(meta.getDatabaseName());
      data.collectionName = environmentSubstitute(meta
          .getCollectionName());

      try {
        if ( StringUtils.isEmpty( data.databaseName )) {
          throw new Exception(BaseMessages.getString(PKG,
              "MongoDBMapReduce.ErrorMessage.NoDBSpecified"));
        }

        if (StringUtils.isEmpty(data.collectionName)) {
          throw new Exception(
              BaseMessages
                  .getString(PKG,
                      "MongoDBMapReduce.ErrorMessage.NoCollectionSpecified"));
        }

        data.outputRowMeta = new RowMeta();
        meta.getFields(data.outputRowMeta, getStepname(), null, null,
            this);
        data.clientWrapper = new MongoClientWrapper(meta,
            this.getParentVariableSpace());

        return true;
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG,
            "MongoDBMapReduce.ErrorConnectingToMongoDb.Exception",
            meta.getServers(), "", data.databaseName,
            data.collectionName), e);
        return false;
      }
    } else {
      return false;
    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
    super.dispose(smi, sdi);
  }

  /**
   * Run is were the action happens.
   */
  public void run() {
    logBasic("Starting to run...");
    try {
      while (processRow(meta, data) && !isStopped()) {
        continue;
      }
    } catch (Exception e) {
      logError("Unexpected error : " + e.toString());
      logError(Const.getStackTracker(e));
      setErrors(1);
      stopAll();
    } finally {
      dispose(meta, data);
      logBasic("Finished, processing " + getLinesRead() + " rows");
      markStop();
    }
  }
}
