package com.ivyis.di.trans.steps.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.ivyis.di.trans.steps.mongodb.wrapper.field.MongoField;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * This class is responsible to processing the data rows.
 * 
 * @author <a href="mailto:joel.latino@ivy-is.co.uk">Joel Latino</a>
 * @since 1.0.0
 */
public class MongoDBLookup extends BaseStep implements StepInterface {
  private static final Class<?> PKG = MongoDBLookup.class;

  private MongoDBLookupMeta meta;
  private MongoDBLookupData data;

  public MongoDBLookup(
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

    this.meta = (MongoDBLookupMeta) smi;
    this.data = (MongoDBLookupData) sdi;

    final Object[] r = getRow();
    if (r == null) {
      data.clientWrapper.dispose();
      setOutputDone();
      return false;
    }

    try {
      if (first) {
        data.outputRowMeta = getInputRowMeta().clone();
        data.nrPreFields = data.outputRowMeta.size();
        // Get output field types
        meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
        data.init();
        first = false;
      }

      data.keynrs = new int[meta.getStreamKeyField1().length];

      QueryBuilder qb = QueryBuilder.start();
      for (int i = 0; i < meta.getStreamKeyField1().length; i++) {
        data.keynrs[i] = getInputRowMeta().indexOfValue(meta.getStreamKeyField1()[i]);
        final String colKey = meta.getCollectionKeyField()[i];

        if (MongoDBLookupMeta.CONDITION_STRINGS[MongoDBLookupMeta.CONDITION_EQ]
            .equalsIgnoreCase(meta.getKeyCondition()[i])) {
          qb = qb.and(colKey).is(r[data.keynrs[i]]);
        }
        if (MongoDBLookupMeta.CONDITION_STRINGS[MongoDBLookupMeta.CONDITION_GE]
            .equalsIgnoreCase(meta.getKeyCondition()[i])) {
          qb = qb.and(colKey).greaterThanEquals(r[data.keynrs[i]]);
        }
        if (MongoDBLookupMeta.CONDITION_STRINGS[MongoDBLookupMeta.CONDITION_GT]
            .equalsIgnoreCase(meta.getKeyCondition()[i])) {
          qb = qb.and(colKey).greaterThan(r[data.keynrs[i]]);
        }
        if (MongoDBLookupMeta.CONDITION_STRINGS[MongoDBLookupMeta.CONDITION_LE]
            .equalsIgnoreCase(meta.getKeyCondition()[i])) {
          qb = qb.and(colKey).lessThanEquals(r[data.keynrs[i]]);
        }
        if (MongoDBLookupMeta.CONDITION_STRINGS[MongoDBLookupMeta.CONDITION_LT]
            .equalsIgnoreCase(meta.getKeyCondition()[i])) {
          qb = qb.and(colKey).lessThan(r[data.keynrs[i]]);
        }
        if (MongoDBLookupMeta.CONDITION_STRINGS[MongoDBLookupMeta.CONDITION_NE]
            .equalsIgnoreCase(meta.getKeyCondition()[i])) {
          qb = qb.and(colKey).notEquals(r[data.keynrs[i]]);
        }
        if (MongoDBLookupMeta.CONDITION_STRINGS[MongoDBLookupMeta.CONDITION_IS_NOT_NULL]
            .equalsIgnoreCase(meta.getKeyCondition()[i])) {
          qb = qb.and(colKey).exists(true).and(colKey).notEquals(null);
        }

        if (log.isDebug()) {
          logDebug(BaseMessages.getString(PKG, "DatabaseLookup.Log.FieldHasIndex1")
              + meta.getStreamKeyField1()[i]
              + BaseMessages.getString(PKG, "DatabaseLookup.Log.FieldHasIndex2") + data.keynrs[i]);
        }
      }

      final DBCursor cur =
          data.clientWrapper.getCollection(data.databaseName, data.collectionName).find(qb.get());
      if (meta.isEatingRowOnLookupFailure() && cur == null) {
        return false;
      }
      if (cur != null && cur.hasNext()) {
        final DBObject obj = cur.next();
        if (meta.isFailingOnMultipleResults() && cur.hasNext()) {
          putError(getInputRowMeta(), r, 1L, "No lookup found", null, "DBL001");
        }

        final Object[][] outputRows = data.mongoDocumentToKettle(obj, MongoDBLookup.this);

        // there may be more than one row if the paths contain an array unwind
        for (int i = 0; i < outputRows.length; i++) {
          final Object[] outRow = outputRows[i];
          for (int x = 0; x < data.nrPreFields; x++) {
            outRow[x] = r[x];
          }
          putRow(data.outputRowMeta, outRow);
        }
        cur.close();
      }

    } catch (java.lang.Exception e) {
      putError(data.outputRowMeta, r, 1, "No data found for input row", "json",
          "unknown order number");
      return false;
    }

    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    if (super.init(smi, sdi)) {
      meta = (MongoDBLookupMeta) smi;
      data = (MongoDBLookupData) sdi;

      data.databaseName = environmentSubstitute(meta.getDatabaseName());
      data.collectionName = environmentSubstitute(meta.getCollectionName());

      final List<MongoField> mfList = new ArrayList<MongoField>();
      for (int i = 0; i < meta.getReturnValueNewName().length; i++) {
        final MongoField mf = new MongoField();
        mf.defaultValue = meta.getReturnValueDefault()[i];
        mf.mKettleType = ValueMeta.getTypeDesc(meta.getReturnValueDefaultType()[i]);
        mf.mFieldPath = meta.getReturnValueField()[i];
        mf.mFieldName = meta.getReturnValueNewName()[i];
        mfList.add(mf);
      }
      data.setMongoFields(mfList);

      try {
        if (Const.isEmpty(data.databaseName)) {
          throw new Exception(BaseMessages.getString(PKG, "MongoDBLookup.ErrorMessage.NoDBSpecified"));
        }

        if (Const.isEmpty(data.collectionName)) {
          throw new Exception(BaseMessages.getString(PKG, "MongoDBLookup.ErrorMessage.NoCollectionSpecified"));
        }

        data.clientWrapper = new MongoClientWrapper(meta, this.getParentVariableSpace());

        data.clientWrapper = new MongoClientWrapper(meta, this.getParentVariableSpace());

        return true;
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "MongoDBLookup.ErrorConnectingToMongoDb.Exception",
            meta.getServers(), "", data.databaseName, data.collectionName), e);
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

}
