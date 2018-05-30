package com.ivyis.di.trans.steps.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import com.ivyis.di.trans.steps.mongodb.wrapper.field.MongoArrayExpansion;
import com.ivyis.di.trans.steps.mongodb.wrapper.field.MongoField;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * This class contains the methods to set and retrieve the status of the step data.
 * 
 * @author <a href="mailto:joel.latino@ivy-is.co.uk">Joel Latino</a>
 * @since 1.0.0
 */
public class MongoDBMapReduceData extends BaseStepData implements StepDataInterface {

  public MongoClientWrapper clientWrapper;
  public RowMetaInterface outputRowMeta;
  public int nrPrevFields;
  public int rowNumber = 0;
  public Iterator<DBObject> mapReduceResult;

  public String databaseName;
  public String collectionName;
  public String mapFunction;
  public String reduceFunction;
  public String servers;

  private List<MongoField> fields;
  private MongoArrayExpansion mExpansionHandler;
  int nrPreFields;

  public void setMongoFields(List<MongoField> fields) {
    // copy this list
    this.fields = new ArrayList<MongoField>();

    for (MongoField f : fields) {
      this.fields.add(f.copy());
    }
  }

  protected static MongoArrayExpansion checkFieldPaths(List<MongoField> normalFields,
      RowMetaInterface outputRowMeta) throws KettleException {

    // here we check whether there are any full array expansions
    // specified in the paths (via [*]). If so, we want to make sure
    // that only one is present across all paths. E.g. we can handle
    // multiple fields like $.person[*].first, $.person[*].last etc.
    // but not $.person[*].first, $.person[*].address[*].street.

    String expansion = null;
    final List<MongoField> normalList = new ArrayList<MongoField>();
    final List<MongoField> expansionList = new ArrayList<MongoField>();

    for (MongoField f : normalFields) {
      final String path = f.mFieldPath;

      if (path != null && path.lastIndexOf("[*]") >= 0) {

        if (path.indexOf("[*]") != path.lastIndexOf("[*]")) {
          throw new KettleException(BaseMessages.getString(MongoDBMapReduceMeta.PKG,
              "MongoInput.ErrorMessage.PathContainsMultipleExpansions", path));
        }

        final String pathPart = path.substring(0, path.lastIndexOf("[*]") + 3);

        if (expansion == null) {
          expansion = pathPart;
        } else {
          if (!expansion.equals(pathPart)) {
            throw new KettleException(BaseMessages.getString(MongoDBMapReduceMeta.PKG,
                "MongoDbInput.ErrorMessage.MutipleDifferentExpansions"));
          }
        }

        expansionList.add(f);
      } else {
        normalList.add(f);
      }
    }

    normalFields.clear();
    for (MongoField f : normalList) {
      normalFields.add(f);
    }

    if (expansionList.size() > 0) {
      final List<MongoField> subFields = new ArrayList<MongoField>();

      for (MongoField ef : expansionList) {
        final MongoField subField = new MongoField();
        subField.mFieldName = ef.mFieldName;
        String path = ef.mFieldPath;
        if (path.charAt(path.length() - 2) == '*') {
          path = "dummy"; // pulling a primitive out of the array (path
          // doesn't matter)
        } else {
          path = path.substring(
              path.lastIndexOf("[*]") + 3, path.length());
          path = "$" + path;
        }

        subField.mFieldPath = path;
        subField.mIndexedVals = ef.mIndexedVals;
        subField.mKettleType = ef.mKettleType;

        subFields.add(subField);
      }

      final MongoArrayExpansion exp = new MongoArrayExpansion(subFields);
      exp.mExpansionPath = expansion;
      exp.mOutputRowMeta = outputRowMeta;

      return exp;
    }

    return null;
  }

  /**
   * Initialize all the paths by locating the index for their field name in the outgoing row
   * structure.
   * 
   * @throws KettleException
   */
  public void init() throws KettleException {
    if (fields != null) {

      // set up array expansion/unwinding (if necessary)
      mExpansionHandler = checkFieldPaths(fields, outputRowMeta);

      for (MongoField f : fields) {
        final int outputIndex = outputRowMeta.indexOfValue(f.mFieldName);
        f.init(outputIndex);
      }

      if (mExpansionHandler != null) {
        mExpansionHandler.init();
      }
    }
  }

  /**
   * Convert a mongo document to outgoing row field values with respect to the user-specified paths.
   * May return more than one Kettle row if an array is being expanded/unwound
   * 
   * @param mongoObj the mongo document
   * @param space variables to use
   * @return populated Kettle row(s)
   * @throws KettleException if a problem occurs
   */
  public Object[][] mongoDocumentToKettle(DBObject mongoObj, VariableSpace space)
      throws KettleException {

    Object[][] result = null;

    if (mExpansionHandler != null) {
      mExpansionHandler.reset(space);

      if (mongoObj instanceof BasicDBObject) {
        result = mExpansionHandler.convertToKettleValue((BasicDBObject) mongoObj, space);
      } else {
        result = mExpansionHandler.convertToKettleValue((BasicDBList) mongoObj, space);
      }
    } else {
      result = new Object[1][];
    }

    // get the normal (non expansion-related fields)
    final Object[] normalData = RowDataUtil.allocateRowData(outputRowMeta.size());
    Object value;
    for (MongoField f : fields) {
      value = null;
      f.reset(space);

      if (mongoObj instanceof BasicDBObject) {
        value = f.convertToKettleValue((BasicDBObject) mongoObj);
      } else if (mongoObj instanceof BasicDBList) {
        value = f.convertToKettleValue((BasicDBList) mongoObj);
      }

      normalData[f.mOutputIndex] = value;
    }

    // copy normal fields over to each expansion row (if necessary)
    if (mExpansionHandler == null) {
      result[0] = normalData;
    } else {
      for (int i = 0; i < result.length; i++) {
        final Object[] row = result[i];
        for (MongoField f : fields) {
          row[f.mOutputIndex] = normalData[f.mOutputIndex];
        }
      }
    }

    return result;
  }

  public static boolean discoverFields(final MongoDBMapReduceMeta meta, final VariableSpace vars,
      final int docsToSample) throws KettleException {
    MongoClientWrapper clientWrapper;
    try {
      clientWrapper = new MongoClientWrapper(meta, vars);
    } catch (NumberFormatException e1) {
      throw new KettleException("MongoDB port number format", e1);
    } catch (UnknownHostException e1) {
      throw new KettleException("Unknown MongoDB hostname", e1);
    }
    try {
      final String db = vars.environmentSubstitute(meta.getDatabaseName());
      final String collection = vars.environmentSubstitute(meta.getCollectionName());
      final String mapFunction = vars.environmentSubstitute(meta.getMapFunction());
      final String reduceFunction = vars.environmentSubstitute(meta.getReduceFunction());
      int numDocsToSample = docsToSample;
      if (numDocsToSample < 1) {
        numDocsToSample = 100; // default
      }
      final List<MongoField> discoveredFields =
          clientWrapper
              .discoverFields(db, collection, mapFunction, reduceFunction, numDocsToSample);

      // return true if query resulted in documents being returned and fields getting extracted
      if (discoveredFields.size() > 0) {
        meta.setFields(discoveredFields);
        return true;
      }
    } catch (Exception e) {
      if (e instanceof KettleException) {
        throw (KettleException) e;
      } else {
        throw new KettleException("Unable to discover fields from MongoDB", e);
      }
    } finally {
      if (clientWrapper != null) {
        clientWrapper.dispose();
      }
    }

    return false;
  }

  /**
   * Cleanses a string path by ensuring that any variables names present in the path do not contain
   * "."s (replaces any dots with underscores).
   * 
   * @param path the path to cleanse
   * @return the cleansed path
   */
  public static String cleansePath(String path) {
    // look for variables and convert any "." to "_"
    int index = path.indexOf("${");

    int endIndex = 0;
    String tempStr = path;
    while (index >= 0) {
      index += 2;
      endIndex += tempStr.indexOf("}");
      if (endIndex > 0 && endIndex > index + 1) {
        final String key = path.substring(index, endIndex);

        final String cleanKey = key.replace('.', '_');
        path = path.replace(key, cleanKey);
      } else {
        break;
      }

      if (endIndex + 1 < path.length()) {
        tempStr = path.substring(endIndex + 1, path.length());
      } else {
        break;
      }

      index = tempStr.indexOf("${");

      if (index > 0) {
        index += endIndex;
      }
    }

    return path;
  }

}
