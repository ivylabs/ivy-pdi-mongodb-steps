package com.ivyis.di.trans.steps.mongodb.wrapper.field;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.types.Binary;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;

import com.ivyis.di.trans.steps.mongodb.MongoDBMapReduceData;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

/**
 * Class responsible for describe a MongoDB field in PDI.
 * 
 * @author <a href="mailto:joel.latino@ivy-is.co.uk">Joel Latino</a>
 * @since 1.0.0
 */
public class MongoField implements Comparable<MongoField> {
  protected static final Class<?> PKG = MongoField.class; // for i18n purposes

  /** The name the the field will take in the outputted kettle stream. */
  public String mFieldName = "";

  /** The path to the field in the Mongo object. */
  public String mFieldPath = "";

  /** The kettle type for this field. */
  public String mKettleType = "";
  public String defaultValue = "";

  /** User-defined indexed values for String types. */
  public List<String> mIndexedVals;

  /**
   * Temporary variable to hold the min:max array index info for fields determined when sampling
   * documents for paths/types.
   */
  public transient String mArrayIndexInfo;

  /**
   * Temporary variable to hold the number of times this path was seen when sampling documents to
   * determine paths/types.
   */
  public transient int mPercentageOfSample = -1;

  /**
   * Temporary variable to hold the num times this path was seen/num sampled documents. Note that
   * numerator might be larger than denominator if this path is encountered multiple times in an
   * array within one document.
   */
  public transient String mOccurenceFraction = "";

  public transient Class<?> mMongoType;

  /**
   * Temporary variable used to indicate that this path occurs multiple times over the sampled
   * documents and that the types differ. In this case we should default to Kettle type String as
   * acatch-all.
   */
  public transient boolean mDisparateTypes;

  /** The index that this field is in the output row structure. */
  public int mOutputIndex;

  private ValueMetaInterface mTempValueMeta;

  private List<String> mPathParts;
  private List<String> mTempParts;

  public MongoField copy() {
    final MongoField newF = new MongoField();
    newF.mFieldName = mFieldName;
    newF.mFieldPath = mFieldPath;
    newF.mKettleType = mKettleType;

    // reference doesn't matter here as this list is read only at runtime
    newF.mIndexedVals = mIndexedVals;

    return newF;
  }

  /**
   * Initialize this mongo field.
   * 
   * @param outputIndex the index for this field in the outgoing row structure.
   * @throws KettleException if a problem occurs
   */
  public void init(int outputIndex) throws KettleException {
    if (Const.isEmpty(mFieldPath)) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDbOutput.Messages.MongoField.Error.NoPathSet"));
    }

    if (mPathParts != null) {
      return;
    }

    final String fieldPath = MongoDBMapReduceData.cleansePath(mFieldPath);

    final String[] temp = fieldPath.split("\\.");
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
    mTempValueMeta = ValueMetaFactory.createValueMeta(ValueMeta
        .getType(mKettleType));
    mOutputIndex = outputIndex;
  }

  /**
   * Reset this field, ready for processing a new document.
   * 
   * @param space variables to use
   */
  public void reset(VariableSpace space) {
    // first clear because there may be stuff left over from processing the previous mongo document
    // object (especially if a path exited early due to non-existent field or array index out of
    // bounds)
    mTempParts.clear();
    for (String part : mPathParts) {
      mTempParts.add(space.environmentSubstitute(part));
    }
  }

  /**
   * Perform Kettle type conversions for the Mongo leaf field value.
   * 
   * @param fieldValue the leaf value from the Mongo structure.
   * @return an Object of the appropriate Kettle type.
   * @throws KettleException if a problem occurs.
   */
  public Object getKettleValue(Object fieldValue) throws KettleException {

    switch (mTempValueMeta.getType()) {
      case ValueMetaInterface.TYPE_BIGNUMBER:
        if (fieldValue instanceof Number) {
          fieldValue = BigDecimal.valueOf(((Number) fieldValue).doubleValue());
        } else if (fieldValue instanceof Date) {
          fieldValue = new BigDecimal(((Date) fieldValue).getTime());
        } else {
          fieldValue = new BigDecimal(fieldValue.toString());
        }
        return mTempValueMeta.getBigNumber(fieldValue);
      case ValueMetaInterface.TYPE_BINARY:
        if (fieldValue instanceof Binary) {
          fieldValue = ((Binary) fieldValue).getData();
        } else {
          fieldValue = fieldValue.toString().getBytes();
        }
        return mTempValueMeta.getBinary(fieldValue);
      case ValueMetaInterface.TYPE_BOOLEAN:
        if (fieldValue instanceof Number) {
          fieldValue = new Boolean(((Number) fieldValue).intValue() != 0);
        } else if (fieldValue instanceof Date) {
          fieldValue = new Boolean(((Date) fieldValue).getTime() != 0);
        } else {
          fieldValue =
              new Boolean(fieldValue.toString().equalsIgnoreCase("Y")
                  || fieldValue.toString().equalsIgnoreCase("T")
                  || fieldValue.toString().equalsIgnoreCase("1"));
        }
        return mTempValueMeta.getBoolean(fieldValue);
      case ValueMetaInterface.TYPE_DATE:
        if (fieldValue instanceof Number) {
          fieldValue = new Date(((Number) fieldValue).longValue());
        } else if (fieldValue instanceof Date) {
          // nothing to do
        } else {
          return "".equals(defaultValue) ? null : defaultValue;
        }
        return mTempValueMeta.getDate(fieldValue);
      case ValueMetaInterface.TYPE_INTEGER:
        if (fieldValue instanceof Number) {
          fieldValue = new Long(((Number) fieldValue).intValue());
        } else if (fieldValue instanceof Binary) {
          final byte[] b = ((Binary) fieldValue).getData();
          final String s = new String(b);
          fieldValue = new Integer(s);
        } else {
          fieldValue = new Integer(fieldValue.toString());
        }
        return mTempValueMeta.getInteger(fieldValue);
      case ValueMetaInterface.TYPE_NUMBER:
        if (fieldValue instanceof Number) {
          fieldValue = new Double(((Number) fieldValue).doubleValue());
        } else if (fieldValue instanceof Binary) {
          final byte[] b = ((Binary) fieldValue).getData();
          final String s = new String(b);
          fieldValue = new Double(s);
        } else {
          fieldValue = new Double(fieldValue.toString());
        }
        return mTempValueMeta.getNumber(fieldValue);
      case ValueMetaInterface.TYPE_STRING:
        return mTempValueMeta.getString(fieldValue);
      default:
        return "".equals(defaultValue) ? null : defaultValue;
    }
  }

  /**
   * Convert a mongo record object to a Kettle field value (for the field defined by this path).
   * 
   * @param mongoObject the record to convert.
   * @return the kettle field value.
   * @throws KettleException if a problem occurs.
   */
  public Object convertToKettleValue(BasicDBObject mongoObject) throws KettleException {

    if (mongoObject == null) {
      return null;
    }

    if (mTempParts.size() == 0) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDbInput.ErrorMessage.MalformedPathRecord"));
    }

    String part = mTempParts.remove(0);

    if (part.charAt(0) == '[') {
      // we're not expecting an array at this point - this document does not contain our field
      return null;
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
      return null;
    }

    // what have we got
    if (mTempParts.size() == 0) {
      // we're expecting a leaf primitive - lets see if that's what we have here...
      return getKettleValue(fieldValue);
    }

    if (fieldValue instanceof BasicDBObject) {
      return convertToKettleValue(((BasicDBObject) fieldValue));
    }

    if (fieldValue instanceof BasicDBList) {
      return convertToKettleValue(((BasicDBList) fieldValue));
    }

    // must mean we have a primitive here, but we're expecting to process more path so this doesn't
    // match us - return null
    return null;
  }

  /**
   * Convert a mongo array object to a Kettle field value (for the field defined in this path).
   * 
   * @param mongoList the array to convert.
   * @return the kettle field value.
   * @throws KettleException if a problem occurs.
   */
  public Object convertToKettleValue(BasicDBList mongoList) throws KettleException {

    if (mongoList == null) {
      return null;
    }

    if (mTempParts.size() == 0) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDbInput.ErrorMessage.MalformedPathArray"));
    }

    String part = mTempParts.remove(0);
    if (!(part.charAt(0) == '[')) {
      // we're expecting an array at this point - this document does not contain our field
      return null;
    }

    final String index = part.substring(1, part.indexOf(']'));
    int arrayI = 0;
    try {
      arrayI = Integer.parseInt(index.trim());
    } catch (NumberFormatException e) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDbInput.ErrorMessage.UnableToParseArrayIndex", index));
    }

    if (part.indexOf(']') < part.length() - 1) {
      // more dimensions to the array
      part = part.substring(part.indexOf(']') + 1, part.length());
      mTempParts.add(0, part);
    }

    if (arrayI >= mongoList.size() || arrayI < 0) {
      return null;
    }

    final Object element = mongoList.get(arrayI);

    if (element == null) {
      return null;
    }

    if (mTempParts.size() == 0) {
      // we're expecting a leaf primitive - let's see if that's what we have here...
      return getKettleValue(element);
    }

    if (element instanceof BasicDBObject) {
      return convertToKettleValue(((BasicDBObject) element));
    }

    if (element instanceof BasicDBList) {
      return convertToKettleValue(((BasicDBList) element));
    }

    // must mean we have a primitive here, but we're expecting to process more path so this doesn't
    // match us - return null
    return null;
  }

  public int compareTo(MongoField comp) {
    return mFieldName.compareTo(comp.mFieldName);
  }
}
