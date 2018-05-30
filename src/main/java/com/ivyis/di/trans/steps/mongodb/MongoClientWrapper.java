package com.ivyis.di.trans.steps.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.apache.commons.lang.StringUtils;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;

import com.ivyis.di.trans.steps.mongodb.wrapper.field.MongoField;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;

/**
 * The MongoDB connection client wrapper.
 * 
 * @author <a href="mailto:joel.latino@ivy-is.co.uk">Joel Latino</a>
 * @since 1.0.0
 */
public class MongoClientWrapper {
  private static final Class<?> PKG = MongoClientWrapper.class;
  public static final String MONGODB_DEFAUL_PORT = "27017";

  private String url;
  private MongoClient mongoClient;
  private MongoDBMeta meta;
  private VariableSpace vars;

  public MongoClientWrapper(MongoDBMeta meta, VariableSpace vars)
      throws UnknownHostException {
    this.meta = meta;
    this.vars = vars;

    String username = vars.environmentSubstitute( meta.getUsername() );
    String password = vars.environmentSubstitute( meta.getPassword() );
    String servers = vars.environmentSubstitute( meta.getServers() );
    String authDb = vars.environmentSubstitute( meta.getAuthDb() );
    String authMechanism = vars.environmentSubstitute( meta.getAuthMechanism() );


    this.url = "mongodb://";
    if ( StringUtils.isNotEmpty( username )) {
      url+=username;
      if (StringUtils.isNotEmpty( password )) {
        url+=":"+password;
      }
      url+="@";
    }
    if (StringUtils.isNotEmpty( servers )) {
      url+=servers;
    }
    if (StringUtils.isNotEmpty( authDb )) {
      url+="/?authSource="+authDb;
    }
    if ( StringUtils.isNotEmpty(authMechanism)){
      url+="&authMechanism="+authMechanism;
    }

    LogChannel.GENERAL.logBasic("MONGO URI : "+url);

    MongoClientURI clientUri = new MongoClientURI( url );
    this.mongoClient = new MongoClient(clientUri);

  }

  public List<String> getDatabaseNames() {
    return mongoClient.getDatabaseNames();
  }

  public void dispose() {
    mongoClient.close();
  }

  public Set<String> getCollectionsNames(String dB) {
    final DB db = getDb(dB);
    return db.getCollectionNames();
  }

  public DB getDb(String db) {
    return mongoClient.getDB(db);
  }

  public DBCollection getCollection(String db, String collection) throws KettleException {
    final DB database = getDb(db);

    if (Const.isEmpty(collection)) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoNoAuthWrapper.ErrorMessage.NoCollectionSpecified"));
    }
    return database.getCollection(collection);
  }

  public MapReduceOutput getMapReduceResult(String db, String collection, String mapFunction,
      String reduceFunction) throws KettleException {
    final DB database = getDb(db);

    if (Const.isEmpty(collection)) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoNoAuthWrapper.ErrorMessage.NoCollectionSpecified"));
    }
    final DBCollection dbcollection = database.getCollection(collection);

    final MapReduceCommand cmd =
        new MapReduceCommand(dbcollection, mapFunction, reduceFunction, null,
            MapReduceCommand.OutputType.INLINE, null);
    return dbcollection.mapReduce(cmd);

  }

  public List<MongoField> discoverFields(String db, String collection, String mapFunction,
      String reduceFunction, int docsToSample) throws KettleException {
    try {
      int numDocsToSample = docsToSample;
      if (numDocsToSample < 1) {
        numDocsToSample = 100; // default
      }

      final List<MongoField> discoveredFields = new ArrayList<MongoField>();
      final Map<String, MongoField> fieldLookup = new HashMap<String, MongoField>();
      try {
        final DB database = getDb(db);

        if (Const.isEmpty(collection)) {
          throw new KettleException(BaseMessages.getString(PKG,
              "MongoNoAuthWrapper.ErrorMessage.NoCollectionSpecified"));
        }
        final DBCollection dbcollection = database.getCollection(collection);

        final MapReduceCommand cmd =
            new MapReduceCommand(dbcollection, mapFunction, reduceFunction, null,
                MapReduceCommand.OutputType.INLINE, null);
        cmd.setLimit(docsToSample);
        final MapReduceOutput out = dbcollection.mapReduce(cmd);

        int actualCount = 0;
        for (DBObject dbObj : out.results()) {
          actualCount++;
          docToFields(dbObj, fieldLookup);
        }

        postProcessPaths(fieldLookup, discoveredFields, actualCount);

        return discoveredFields;
      } catch (Exception e) {
        throw new KettleException(e);
      }
    } catch (Exception ex) {
      if (ex instanceof KettleException) {
        throw (KettleException) ex;
      } else {
        throw new KettleException(BaseMessages.getString(PKG,
            "MongoNoAuthWrapper.ErrorMessage.UnableToDiscoverFields"), ex);
      }
    }
  }

  protected static void docToFields(DBObject doc, Map<String, MongoField> lookup) {
    final String root = "$";
    final String name = "$";

    if (doc instanceof BasicDBObject) {
      processRecord((BasicDBObject) doc, root, name, lookup);
    } else if (doc instanceof BasicDBList) {
      processList((BasicDBList) doc, root, name, lookup);
    }
  }

  private static void processRecord(BasicDBObject rec, String path, String name,
      Map<String, MongoField> lookup) {
    for (String key : rec.keySet()) {
      final Object fieldValue = rec.get(key);

      if (fieldValue instanceof BasicDBObject) {
        processRecord((BasicDBObject) fieldValue, path + "." + key, name + "." + key, lookup);
      } else if (fieldValue instanceof BasicDBList) {
        processList((BasicDBList) fieldValue, path + "." + key, name + "." + key, lookup);
      } else {
        // some sort of primitive
        final String finalPath = path + "." + key;
        final String finalName = name + "." + key;
        if (!lookup.containsKey(finalPath)) {
          final MongoField newField = new MongoField();
          final int kettleType = mongoToKettleType(fieldValue);

          // Following suit of mongoToKettleType by interpreting nullas String type
          newField.mMongoType = String.class;
          if (fieldValue != null) {
            newField.mMongoType = fieldValue.getClass();
          }
          newField.mFieldName = finalName;
          newField.mFieldPath = finalPath;
          newField.mKettleType = ValueMetaFactory.getValueMetaName(kettleType);
          newField.mPercentageOfSample = 1;

          lookup.put(finalPath, newField);
        } else {
          // update max indexes in array parts of name
          final MongoField m = lookup.get(finalPath);
          Class<?> fieldClass = String.class;
          if (fieldValue != null) {
            fieldClass = fieldValue.getClass();
          }
          if (!m.mMongoType.isAssignableFrom(fieldClass)) {
            m.mDisparateTypes = true;
          }
          m.mPercentageOfSample++;
          updateMaxArrayIndexes(m, finalName);
        }
      }
    }
  }

  protected static int mongoToKettleType(Object fieldValue) {
    if (fieldValue == null) {
      return ValueMetaInterface.TYPE_STRING;
    }

    if (fieldValue instanceof Symbol || fieldValue instanceof String || fieldValue instanceof Code
        || fieldValue instanceof ObjectId || fieldValue instanceof MinKey
        || fieldValue instanceof MaxKey) {
      return ValueMetaInterface.TYPE_STRING;
    } else if (fieldValue instanceof Date) {
      return ValueMetaInterface.TYPE_DATE;
    } else if (fieldValue instanceof Number) {
      // try to parse as an Integer
      try {
        Integer.parseInt(fieldValue.toString());
        return ValueMetaInterface.TYPE_INTEGER;
      } catch (NumberFormatException e) {
        return ValueMetaInterface.TYPE_NUMBER;
      }
    } else if (fieldValue instanceof Binary) {
      return ValueMetaInterface.TYPE_BINARY;
    } else if (fieldValue instanceof BSONTimestamp) {
      return ValueMetaInterface.TYPE_INTEGER;
    }

    return ValueMetaInterface.TYPE_STRING;
  }

  private static void processList(BasicDBList list, String path, String name,
      Map<String, MongoField> lookup) {

    if (list.size() == 0) {
      return; // can't infer anything about an empty list
    }

    final String nonPrimitivePath = path + "[-]";
    final String primitivePath = path;

    for (int i = 0; i < list.size(); i++) {
      final Object element = list.get(i);

      if (element instanceof BasicDBObject) {
        processRecord((BasicDBObject) element, nonPrimitivePath, name + "[" + i + ":" + i + "]",
            lookup);
      } else if (element instanceof BasicDBList) {
        processList((BasicDBList) element, nonPrimitivePath, name + "[" + i + ":" + i + "]",
            lookup);
      } else {
        // some sort of primitive
        final String finalPath = primitivePath + "[" + i + "]";
        final String finalName = name + "[" + i + "]";
        if (!lookup.containsKey(finalPath)) {
          final MongoField newField = new MongoField();
          final int kettleType = mongoToKettleType(element);

          // Following suit of mongoToKettleType by interpreting null as String type
          newField.mMongoType = String.class;
          if (element != null) {
            newField.mMongoType = element.getClass();
          }
          newField.mFieldName = finalPath;
          newField.mFieldPath = finalName;
          newField.mKettleType = ValueMetaFactory.getValueMetaName(kettleType);
          newField.mPercentageOfSample = 1;

          lookup.put(finalPath, newField);
        } else {
          // update max indexes in array parts of name
          final MongoField m = lookup.get(finalPath);
          Class<?> elementClass = String.class;
          if (element != null) {
            elementClass = element.getClass();
          }
          if (!m.mMongoType.isAssignableFrom(elementClass)) {
            m.mDisparateTypes = true;
          }
          m.mPercentageOfSample++;
          updateMaxArrayIndexes(m, finalName);
        }
      }
    }
  }

  protected static void postProcessPaths(Map<String, MongoField> fieldLookup,
      List<MongoField> discoveredFields, int numDocsProcessed) {
    for (String key : fieldLookup.keySet()) {
      final MongoField m = fieldLookup.get(key);
      m.mOccurenceFraction = "" + m.mPercentageOfSample + "/" + numDocsProcessed;
      setMinArrayIndexes(m);

      // set field names to terminal part and copy any min:max array index info
      if (m.mFieldName.contains("[") && m.mFieldName.contains(":")) {
        m.mArrayIndexInfo = m.mFieldName;
      }
      if (m.mFieldName.indexOf('.') >= 0) {
        m.mFieldName = m.mFieldName.substring(
            m.mFieldName.lastIndexOf('.') + 1,
            m.mFieldName.length());
      }

      if (m.mDisparateTypes) {
        // force type to string if we've seen this path more than once with incompatible types
        m.mKettleType = new ValueMetaString().getTypeDesc();
      }
      discoveredFields.add(m);
    }

    // check for name clashes
    final Map<String, Integer> tempM = new HashMap<String, Integer>();
    for (MongoField m : discoveredFields) {
      if (tempM.get(m.mFieldName) != null) {
        Integer toUse = tempM.get(m.mFieldName);
        final String key = m.mFieldName;
        m.mFieldName = key + "_" + toUse;
        toUse = new Integer(toUse.intValue() + 1);
        tempM.put(key, toUse);
      } else {
        tempM.put(m.mFieldName, 1);
      }
    }
  }

  protected static void setMinArrayIndexes(MongoField m) {
    // set the actual index for each array in the path to the corresponding minimum index recorded
    // in the name
    if (m.mFieldName.indexOf('[') < 0) {
      return;
    }

    String temp = m.mFieldPath;
    String tempComp = m.mFieldName;
    final StringBuffer updated = new StringBuffer();

    while (temp.indexOf('[') >= 0) {
      final String firstPart = temp.substring(0, temp.indexOf('['));
      final String innerPart = temp.substring(temp.indexOf('[') + 1, temp.indexOf(']'));

      if (!innerPart.equals("-")) {
        // terminal primitive specific index
        updated.append(temp); // finished
        temp = "";
        break;
      } else {
        updated.append(firstPart);

        final String innerComp =
            tempComp.substring(tempComp.indexOf('[') + 1, tempComp.indexOf(']'));

        if (temp.indexOf(']') < temp.length() - 1) {
          temp = temp.substring(temp.indexOf(']') + 1, temp.length());
          tempComp = tempComp.substring(tempComp.indexOf(']') + 1,
              tempComp.length());
        } else {
          temp = "";
        }

        final String[] compParts = innerComp.split(":");
        final String replace = "[" + compParts[0] + "]";
        updated.append(replace);

      }
    }

    if (temp.length() > 0) {
      // append remaining part
      updated.append(temp);
    }

    m.mFieldPath = updated.toString();
  }

  protected static void updateMaxArrayIndexes(MongoField m, String update) {
    // just look at the second (i.e. max index value) in the array partsof update
    if (m.mFieldName.indexOf('[') < 0) {
      return;
    }

    if (m.mFieldName.split("\\[").length != update.split("\\[").length) {
      throw new IllegalArgumentException("Field path and update path do not seem to contain "
          + "the same number of array parts!");
    }

    String temp = m.mFieldName;
    String tempComp = update;
    final StringBuffer updated = new StringBuffer();

    while (temp.indexOf('[') >= 0) {
      final String firstPart = temp.substring(0, temp.indexOf('['));
      final String innerPart = temp.substring(temp.indexOf('[') + 1, temp.indexOf(']'));

      if (innerPart.indexOf(':') < 0) {
        // terminal primitive specific index
        updated.append(temp); // finished
        temp = "";
        break;
      } else {
        updated.append(firstPart);

        final String innerComp =
            tempComp.substring(tempComp.indexOf('[') + 1, tempComp.indexOf(']'));

        if (temp.indexOf(']') < temp.length() - 1) {
          temp = temp.substring(temp.indexOf(']') + 1, temp.length());
          tempComp = tempComp.substring(tempComp.indexOf(']') + 1, tempComp.length());
        } else {
          temp = "";
        }

        final String[] origParts = innerPart.split(":");
        final String[] compParts = innerComp.split(":");
        final int origMax = Integer.parseInt(origParts[1]);
        final int compMax = Integer.parseInt(compParts[1]);

        if (compMax > origMax) {
          // updated the max index seen for this path
          final String newRange = "[" + origParts[0] + ":" + compMax + "]";
          updated.append(newRange);
        } else {
          final String oldRange = "[" + innerPart + "]";
          updated.append(oldRange);
        }
      }
    }

    if (temp.length() > 0) {
      // append remaining part
      updated.append(temp);
    }

    m.mFieldName = updated.toString();
  }

  public List<MongoField> discoverFields(String db, String collection, int numDocsToSample)
      throws KettleException {
    DBCursor cursor = null;
    try {
      if (numDocsToSample < 1) {
        numDocsToSample = 100; // default
      }

      final List<MongoField> discoveredFields = new ArrayList<MongoField>();
      final Map<String, MongoField> fieldLookup = new HashMap<String, MongoField>();
      try {
        final DB database = getDb(db);

        if (Const.isEmpty(collection)) {
          throw new KettleException(BaseMessages.getString(PKG,
              "MongoNoAuthWrapper.ErrorMessage.NoCollectionSpecified"));
        }
        final DBCollection dbcollection = database.getCollection(collection);

        cursor = dbcollection.find().limit(numDocsToSample);

        int actualCount = 0;
        while (cursor != null && cursor.hasNext()) {
          actualCount++;
          final DBObject nextDoc = cursor.next();
          docToFields(nextDoc, fieldLookup);
        }

        postProcessPaths(fieldLookup, discoveredFields, actualCount);

        return discoveredFields;
      } catch (Exception e) {
        throw new KettleException(e);
      } finally {
        if (cursor != null) {
          cursor.close();
        }
      }
    } catch (Exception ex) {
      if (ex instanceof KettleException) {
        throw (KettleException) ex;
      } else {
        throw new KettleException(BaseMessages.getString(PKG,
            "MongoNoAuthWrapper.ErrorMessage.UnableToDiscoverFields"), ex);
      }
    }
  }
}
