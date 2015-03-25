package com.ivyis.di.trans.steps.mongodb;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
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

/**
 * This class is responsible to processing the data rows.
 * 
 * @author <a href="mailto:joel.latino@ivy-is.co.uk">Joel Latino</a>
 * @since 1.0.0
 */
public class MongoDBMapReduce extends BaseStep implements StepInterface {
  private static final Class<?> PKG = MongoDBMapReduce.class;

  private MongoDBMapReduceMeta meta;
  private MongoDBMapReduceData data;

  public MongoDBMapReduce(
      StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
    super(s, stepDataInterface, c, t, dis);
  }

  /**
   * {@inheritDoc}
   * 
   * @throws KettleException
   */
  @Override
  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
    try {
      if (first) {
        first = false;
        data.outputRowMeta = new RowMeta();
        meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
        data.nrPrevFields = data.outputRowMeta.size();
      }

      if (!data.mapReduceResult.hasNext()) {
        data.clientWrapper.dispose();
        setOutputDone(); // signal end to receiver(s)
        logDebug("Finished, processing all rows of mongodb map/reduce command");
        return false; // end of data or error.
      } else {
        Object row[] = null;

        final DBObject nextDoc = data.mapReduceResult.next();

        if (meta.isOutputAsJson() || meta.getFields() == null || meta.getFields().size() == 0) {
          final String json = nextDoc.toString();
          row = RowDataUtil.allocateRowData(data.outputRowMeta.size());
          int index = 0;

          row[index++] = json;
          putRow(data.outputRowMeta, row);
        } else {
          final Object[][] outputRows = data.mongoDocumentToKettle(nextDoc, MongoDBMapReduce.this);

          // there may be more than one row if the paths contain an array unwind
          for (int i = 0; i < outputRows.length; i++) {
            putRow(data.outputRowMeta, outputRows[i]);
          }
        }

      }
      return true;

    } catch (Exception e) {
      if (e instanceof KettleException) {
        throw (KettleException) e;
      } else {
        throw new KettleException(e);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    if (super.init(smi, sdi)) {
      meta = (MongoDBMapReduceMeta) smi;
      data = (MongoDBMapReduceData) sdi;

      data.hostname = environmentSubstitute(meta.getHostname());
      data.port =
          Const.toInt(environmentSubstitute(meta.getPort()),
              Integer.parseInt(MongoClientWrapper.MONGODB_DEFAUL_PORT));
      data.databaseName = environmentSubstitute(meta.getDatabaseName());
      data.collectionName = environmentSubstitute(meta.getCollectionName());
      data.mapFunction = environmentSubstitute(meta.getMapFunction());
      data.reduceFunction = environmentSubstitute(meta.getReduceFunction());
      data.setMongoFields(meta.getFields());

      try {
        if (Const.isEmpty(data.databaseName)) {
          throw new Exception(BaseMessages.getString(PKG,
              "MongoDBMapReduce.ErrorMessage.NoDBSpecified"));
        }

        if (Const.isEmpty(data.collectionName)) {
          throw new Exception(BaseMessages.getString(PKG,
              "MongoDBMapReduce.ErrorMessage.NoCollectionSpecified"));
        }

        data.outputRowMeta = new RowMeta();
        meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
        data.clientWrapper = new MongoClientWrapper(meta, this.getParentVariableSpace());
        data.mapReduceResult =
            data.clientWrapper
                .getMapReduceResult(data.databaseName, data.collectionName, data.mapFunction,
                    data.reduceFunction).results().iterator();

        data.init();

        return true;
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "MongoDBMapReduce.ErrorConnectingToMongoDb.Exception",
            data.hostname, "" + data.port, data.databaseName, data.collectionName), e);
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
