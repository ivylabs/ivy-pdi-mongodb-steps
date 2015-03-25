package com.ivyis.di.trans.steps.mongodb.wrapper.field;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;

import com.ivyis.di.trans.steps.mongodb.MongoDBMapReduceData;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

/**
 * Class responsible for processing Json path of MongoDB steps.
 * 
 * @author <a href="mailto:joel.latino@ivy-is.co.uk">Joel Latino</a>
 * @since 1.0.0
 */
public class MongoArrayExpansion {
  protected static final Class<?> PKG = MongoArrayExpansion.class; // for i18n purposes

  /** The prefix of the full path that defines the expansion. */
  public String mExpansionPath;

  /** Subfield objects that handle the processing of the path after the expansion prefix. */
  protected List<MongoField> mSubFields;

  private List<String> mPathParts;
  private List<String> mTempParts;

  public RowMetaInterface mOutputRowMeta;

  public MongoArrayExpansion(List<MongoField> subFields) {
    mSubFields = subFields;
  }

  /**
   * Initialize this field by parsing the path etc.
   * 
   * @throws KettleException if a problem occurs.
   */
  public void init() throws KettleException {
    if (Const.isEmpty(mExpansionPath)) {
      throw new KettleException(BaseMessages.getString(PKG, "MongoDbInput.ErrorMessage.NoPathSet"));
    }
    if (mPathParts != null) {
      return;
    }

    final String expansionPath = MongoDBMapReduceData.cleansePath(mExpansionPath);

    final String[] temp = expansionPath.split("\\.");
    mPathParts = new ArrayList<String>();
    for (String part : temp) {
      mPathParts.add(part);
    }

    if (mPathParts.get(0).equals("$")) {
      mPathParts.remove(0); // root record indicator
    } else if (mPathParts.get(0).startsWith("$[")) {

      // strip leading $ off of array
      final String r = mPathParts.get(0).substring(1, mPathParts.get(0).length());
      mPathParts.set(0, r);
    }
    mTempParts = new ArrayList<String>();

    // initialize the sub fields
    if (mSubFields != null) {
      for (MongoField f : mSubFields) {
        final int outputIndex = mOutputRowMeta.indexOfValue(f.mFieldName);
        f.init(outputIndex);
      }
    }
  }

  /**
   * Reset this field. Should be called prior to processing a new field value from the avro file
   * 
   * @param space environment variables (values that environment variables resolve to cannot contain
   *        "."s)
   */
  public void reset(VariableSpace space) {
    mTempParts.clear();

    for (String part : mPathParts) {
      mTempParts.add(space.environmentSubstitute(part));
    }

    // reset sub fields
    for (MongoField f : mSubFields) {
      f.reset(space);
    }
  }

  protected Object[][] nullResult() {
    final Object[][] result = new Object[1][mOutputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];
    return result;
  }

  public Object[][] convertToKettleValue(BasicDBObject mongoObject, VariableSpace space)
      throws KettleException {

    if (mongoObject == null) {
      return nullResult();
    }

    if (mTempParts.size() == 0) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDbInput.ErrorMessage.MalformedPathRecord"));
    }

    String part = mTempParts.remove(0);

    if (part.charAt(0) == '[') {
      // we're not expecting an array at this point - this document does not contain our field(s)
      return nullResult();
    }

    if (part.indexOf('[') > 0) {
      final String arrayPart = part.substring(part.indexOf('['));
      part = part.substring(0, part.indexOf('['));

      // put the array section back into location zero
      mTempParts.add(0, arrayPart);
    }

    // part is a named field of this record
    final Object fieldValue = mongoObject.get(part);
    if (fieldValue == null) {
      return nullResult();
    }

    if (fieldValue instanceof BasicDBObject) {
      return convertToKettleValue(((BasicDBObject) fieldValue), space);
    }

    if (fieldValue instanceof BasicDBList) {
      return convertToKettleValue(((BasicDBList) fieldValue), space);
    }

    // must mean we have a primitive here, but we're expecting to process more path so this doesn't
    // match us - return null
    return nullResult();
  }

  public Object[][] convertToKettleValue(BasicDBList mongoList, VariableSpace space)
      throws KettleException {

    if (mongoList == null) {
      return nullResult();
    }

    if (mTempParts.size() == 0) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDbInput.ErrorMessage.MalformedPathArray"));
    }

    String part = mTempParts.remove(0);
    if (!(part.charAt(0) == '[')) {
      // we're expecting an array at this point - this document does not contain our field
      return nullResult();
    }

    final String index = part.substring(1, part.indexOf(']'));

    if (part.indexOf(']') < part.length() - 1) {
      // more dimensions to the array
      part = part.substring(part.indexOf(']') + 1, part.length());
      mTempParts.add(0, part);
    }

    if (index.equals("*")) {
      // start the expansion - we delegate conversion to our subfields
      final Object[][] result =
          new Object[mongoList.size()][mOutputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];

      for (int i = 0; i < mongoList.size(); i++) {
        final Object element = mongoList.get(i);

        for (int j = 0; j < mSubFields.size(); j++) {
          final MongoField sf = mSubFields.get(j);
          sf.reset(space);
          // what have we got?
          if (element instanceof BasicDBObject) {
            result[i][sf.mOutputIndex] = sf.convertToKettleValue((BasicDBObject) element);
          } else if (element instanceof BasicDBList) {
            result[i][sf.mOutputIndex] = sf.convertToKettleValue((BasicDBList) element);
          } else {
            // assume a primitive
            result[i][sf.mOutputIndex] = sf.getKettleValue(element);
          }
        }
      }
      return result;
    } else {
      int arrayI = 0;
      try {
        arrayI = Integer.parseInt(index.trim());
      } catch (NumberFormatException e) {
        throw new KettleException(BaseMessages.getString(PKG,
            "MongoDbInput.ErrorMessage.UnableToParseArrayIndex", index));
      }

      if (arrayI >= mongoList.size() || arrayI < 0) {
        // index is out of bounds
        return nullResult();
      }

      final Object element = mongoList.get(arrayI);

      if (element == null) {
        return nullResult();
      }

      if (element instanceof BasicDBObject) {
        return convertToKettleValue(((BasicDBObject) element), space);
      }

      if (element instanceof BasicDBList) {
        return convertToKettleValue(((BasicDBList) element), space);
      }

      // must mean we have a primitive here, but we're expecting to process more path so this
      // doesn't match us - return null
      return nullResult();
    }
  }
}
