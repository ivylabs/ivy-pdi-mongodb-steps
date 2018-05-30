package com.ivyis.di.trans.steps.mongodb;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import com.ivyis.di.ui.trans.steps.mongodb.MongoDBLookupDialog;

/**
 * This class is responsible for implementing functionality regarding step meta. All Kettle steps
 * have an extension of this where private fields have been added with public accessors.
 * 
 * @author <a href="mailto:joel.latino@ivy-is.co.uk">Joel Latino</a>
 * @since 1.0.0
 */
@Step(id = "MongoDBLookup", name = "MongoDBLookup.Step.Name",
    description = "MongoDBLookup.Step.Description",
    categoryDescription = "MongoDBLookup.Step.Category",
    image = "com/ivyis/di/trans/steps/mongodb/MongoDBLookup.png",
    i18nPackageName = "com.ivyis.di.trans.steps.git.operations",
    casesUrl = "https://github.com/ivylabs", documentationUrl = "https://github.com/ivylabs",
    forumUrl = "https://github.com/ivylabs")
public class MongoDBLookupMeta extends MongoDBMeta implements StepMetaInterface {

  public static final String[] CONDITION_STRINGS = new String[] {"=", "<>", "<", "<=", ">", ">=",
      "IS NOT NULL"};

  public static final int CONDITION_EQ = 0;
  public static final int CONDITION_NE = 1;
  public static final int CONDITION_LT = 2;
  public static final int CONDITION_LE = 3;
  public static final int CONDITION_GT = 4;
  public static final int CONDITION_GE = 5;
  public static final int CONDITION_IS_NOT_NULL = 6;

  public static final String MONGODB_DEFAUL_PORT = "27017";

  /** for i18n purposes. **/
  protected static final Class<?> PKG = MongoDBLookupMeta.class;

  /** which field in input stream to compare with. */
  private String[] streamKeyField1 = new String[] {};

  /** Comparator: =, <>, ... */
  private String[] keyCondition = new String[] {};

  /** field in table. */
  private String[] collectionKeyField = new String[] {};

  /** return these field values after lookup. */
  private String[] returnValueField = new String[] {};

  /** new name for value ... */
  private String[] returnValueNewName = new String[] {};

  /** default value in case not found... */
  private String[] returnValueDefault = new String[] {};

  /** type of default value. */
  private int[] returnValueDefaultType = new int[] {};

  /** Have the lookup fail if multiple results were found, renders the orderByClause useless. */
  private boolean failingOnMultipleResults;

  /** Have the lookup eat the incoming row when nothing gets found. */
  private boolean eatingRowOnLookupFailure;

  public String[] getStreamKeyField1() {
    return streamKeyField1;
  }

  public void setStreamKeyField1(String[] streamKeyField1) {
    this.streamKeyField1 = streamKeyField1;
  }

  public String[] getKeyCondition() {
    return keyCondition;
  }

  public void setKeyCondition(String[] keyCondition) {
    this.keyCondition = keyCondition;
  }

  public String[] getCollectionKeyField() {
    return collectionKeyField;
  }

  public void setCollectionKeyField(String[] collectionKeyField) {
    this.collectionKeyField = collectionKeyField;
  }

  public String[] getReturnValueField() {
    return returnValueField;
  }

  public void setReturnValueField(String[] returnValueField) {
    this.returnValueField = returnValueField;
  }

  public String[] getReturnValueNewName() {
    return returnValueNewName;
  }

  public void setReturnValueNewName(String[] returnValueNewName) {
    this.returnValueNewName = returnValueNewName;
  }

  public String[] getReturnValueDefault() {
    return returnValueDefault;
  }

  public void setReturnValueDefault(String[] returnValueDefault) {
    this.returnValueDefault = returnValueDefault;
  }

  public int[] getReturnValueDefaultType() {
    return returnValueDefaultType;
  }

  public void setReturnValueDefaultType(int[] returnValueDefaultType) {
    this.returnValueDefaultType = returnValueDefaultType;
  }

  public boolean isFailingOnMultipleResults() {
    return failingOnMultipleResults;
  }

  public void setFailingOnMultipleResults(boolean failingOnMultipleResults) {
    this.failingOnMultipleResults = failingOnMultipleResults;
  }

  public boolean isEatingRowOnLookupFailure() {
    return eatingRowOnLookupFailure;
  }

  public void setEatingRowOnLookupFailure(boolean eatingRowOnLookupFailure) {
    this.eatingRowOnLookupFailure = eatingRowOnLookupFailure;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getXML() {
    final StringBuilder retval = new StringBuilder();
    retval.append("    " + XMLHandler.addTagValue("servers", servers));
    retval.append("    " + XMLHandler.addTagValue("username", username));
    retval.append("    " + XMLHandler.addTagValue("password", Encr.encryptPasswordIfNotUsingVariables( password )));
    retval.append("    " + XMLHandler.addTagValue("auth_db", authDb));
    retval.append("    " + XMLHandler.addTagValue("auth_mechanism", authMechanism));

    retval.append("    " + XMLHandler.addTagValue("databaseName", databaseName));
    retval.append("    " + XMLHandler.addTagValue("collectionName", collectionName));

    retval.append("    <lookup>").append(Const.CR);
    retval.append("      ").append(
        XMLHandler.addTagValue("fail_on_multiple", failingOnMultipleResults));
    retval.append("      ").append(
        XMLHandler.addTagValue("eat_row_on_failure", eatingRowOnLookupFailure));

    for (int i = 0; i < streamKeyField1.length; i++) {
      retval.append("      <key>").append(Const.CR);
      retval.append("        ").append(XMLHandler.addTagValue("name", streamKeyField1[i]));
      retval.append("        ").append(XMLHandler.addTagValue("field", collectionKeyField[i]));
      retval.append("        ").append(XMLHandler.addTagValue("condition", keyCondition[i]));
      retval.append("      </key>").append(Const.CR);
    }

    for (int i = 0; i < returnValueField.length; i++) {
      retval.append("      <value>").append(Const.CR);
      retval.append("        ").append(XMLHandler.addTagValue("name", returnValueField[i]));
      retval.append("        ").append(XMLHandler.addTagValue("rename", returnValueNewName[i]));
      retval.append("        ").append(XMLHandler.addTagValue("default", returnValueDefault[i]));
      retval.append("        ").append(
          XMLHandler.addTagValue("type", ValueMeta.getTypeDesc(returnValueDefaultType[i])));
      retval.append("      </value>").append(Const.CR);
    }

    retval.append("    </lookup>").append(Const.CR);

    return retval.toString();
  }

  /**
   * Reads data from XML transformation file.
   * 
   * @param stepnode the step XML node.
   * @throws KettleXMLException the kettle XML exception.
   */
  public void readData(Node stepnode) throws KettleXMLException {
    try {
      servers = XMLHandler.getTagValue(stepnode, "servers");
      String hostname = XMLHandler.getTagValue(stepnode, "hostname");
      String port = XMLHandler.getTagValue(stepnode, "hostname");
      if ( StringUtils.isNotEmpty(hostname)) {
        if (StringUtils.isNotEmpty( servers )) {
          servers+=",";
        }
        servers+=hostname;
        if (StringUtils.isNotEmpty(port)) {
          servers+=":"+port;
        }
      }

      username = XMLHandler.getTagValue(stepnode, "username");
      password = Encr.decryptPasswordOptionallyEncrypted(XMLHandler.getTagValue(stepnode, "password"));
      authDb = XMLHandler.getTagValue(stepnode, "auth_db");
      authMechanism = XMLHandler.getTagValue(stepnode, "auth_mechanism");

      databaseName = XMLHandler.getTagValue(stepnode, "databaseName");
      collectionName = XMLHandler.getTagValue(stepnode, "collectionName");

      final Node lookup = XMLHandler.getSubNode(stepnode, "lookup");

      final int nrkeys = XMLHandler.countNodes(lookup, "key");
      final int nrvalues = XMLHandler.countNodes(lookup, "value");

      allocate(nrkeys, nrvalues);

      for (int i = 0; i < nrkeys; i++) {
        final Node knode = XMLHandler.getSubNodeByNr(lookup, "key", i);

        streamKeyField1[i] = XMLHandler.getTagValue(knode, "name");
        collectionKeyField[i] = XMLHandler.getTagValue(knode, "field");
        keyCondition[i] = XMLHandler.getTagValue(knode, "condition");
        if (keyCondition[i] == null) {
          keyCondition[i] = "=";
        }
      }

      for (int i = 0; i < nrvalues; i++) {
        final Node vnode = XMLHandler.getSubNodeByNr(lookup, "value", i);

        returnValueField[i] = XMLHandler.getTagValue(vnode, "name");
        returnValueNewName[i] = XMLHandler.getTagValue(vnode, "rename");
        if (returnValueNewName[i] == null) {
          returnValueNewName[i] = returnValueField[i];
        }
        returnValueDefault[i] = XMLHandler.getTagValue(vnode, "default");
        String dtype = XMLHandler.getTagValue(vnode, "type");
        returnValueDefaultType[i] = ValueMeta.getType(dtype);
        if (returnValueDefaultType[i] < 0) {
          returnValueDefaultType[i] = ValueMetaInterface.TYPE_STRING;
        }
      }
      failingOnMultipleResults =
          "Y".equalsIgnoreCase(XMLHandler.getTagValue(lookup, "fail_on_multiple"));
      eatingRowOnLookupFailure =
          "Y".equalsIgnoreCase(XMLHandler.getTagValue(lookup, "eat_row_on_failure"));
    } catch (Exception e) {
      throw new KettleXMLException(BaseMessages.getString(PKG,
          "MongoDBLookupMeta.Exception.UnexpectedErrorInReadingStepInfo"), e);
    }
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId idStep, List<DatabaseMeta> databases ) throws KettleException {
    try {
      servers = rep.getStepAttributeString(idStep, "servers");
      String hostname = rep.getStepAttributeString(idStep, "hostname");
      String port = rep.getStepAttributeString(idStep, "hostname");
      if ( StringUtils.isNotEmpty(hostname)) {
        if (StringUtils.isNotEmpty( servers )) {
          servers+=",";
        }
        servers+=hostname;
        if (StringUtils.isNotEmpty(port)) {
          servers+=":"+port;
        }
      }

      username = rep.getStepAttributeString(idStep, "username");
      password = Encr.decryptPasswordOptionallyEncrypted( rep.getStepAttributeString(idStep, "password") );
      authDb = rep.getStepAttributeString(idStep, "auth_db");
      authMechanism = rep.getStepAttributeString(idStep, "auth_mechanism");

      databaseName = rep.getStepAttributeString(idStep, "databaseName");
      collectionName = rep.getStepAttributeString(idStep, "collectionName");
      eatingRowOnLookupFailure = rep.getStepAttributeBoolean(idStep, "eat_row_on_failure");

      final int nrkeys = rep.countNrStepAttributes(idStep, "lookup_key_field");
      final int nrvalues = rep.countNrStepAttributes(idStep, "return_value_name");

      allocate(nrkeys, nrvalues);

      for (int i = 0; i < nrkeys; i++) {
        streamKeyField1[i] = rep.getStepAttributeString(idStep, i, "lookup_key_name");
        collectionKeyField[i] = rep.getStepAttributeString(idStep, i, "lookup_key_field");
        keyCondition[i] = rep.getStepAttributeString(idStep, i, "lookup_key_condition");
      }

      for (int i = 0; i < nrvalues; i++) {
        returnValueField[i] = rep.getStepAttributeString(idStep, i, "return_value_name");
        returnValueNewName[i] = rep.getStepAttributeString(idStep, i, "return_value_rename");
        returnValueDefault[i] = rep.getStepAttributeString(idStep, i, "return_value_default");
        returnValueDefaultType[i] =
            ValueMeta.getType(rep.getStepAttributeString(idStep, i, "return_value_type"));
      }
    } catch (Exception e) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDBLookupMeta.Exception.UnexpectedErrorInReadingStepInfo"), e);
    }
  }

  /**
   * {@inheritDoc}
   * 
   * @throws KettleException
   */
  @Override
  public void saveRep(Repository rep, IMetaStore metaStore, ObjectId idTransformation, ObjectId idStep)
      throws KettleException {
    try {
      rep.saveStepAttribute(idTransformation, idStep, "servers", servers);
      rep.saveStepAttribute(idTransformation, idStep, "username", username);
      rep.saveStepAttribute(idTransformation, idStep, "password", password);
      rep.saveStepAttribute(idTransformation, idStep, "auth_db", authDb);
      rep.saveStepAttribute(idTransformation, idStep, "auth_mechanism", authMechanism);

      rep.saveStepAttribute(idTransformation, idStep, "databaseName", databaseName);
      rep.saveStepAttribute(idTransformation, idStep, "collectionName", collectionName);
      rep.saveStepAttribute(idTransformation, idStep, "fail_on_multiple", failingOnMultipleResults);
      rep.saveStepAttribute(idTransformation, idStep, "eat_row_on_failure",
          eatingRowOnLookupFailure);

      for (int i = 0; i < streamKeyField1.length; i++) {
        rep.saveStepAttribute(idTransformation, idStep, i, "lookup_key_name", streamKeyField1[i]);
        rep.saveStepAttribute(idTransformation, idStep, i, "lookup_key_field",
            collectionKeyField[i]);
        rep.saveStepAttribute(idTransformation, idStep, i, "lookup_key_condition", keyCondition[i]);
      }

      for (int i = 0; i < returnValueField.length; i++) {
        rep.saveStepAttribute(idTransformation, idStep, i, "return_value_name",
            returnValueField[i]);
        rep.saveStepAttribute(idTransformation, idStep, i, "return_value_rename",
            returnValueNewName[i]);
        rep.saveStepAttribute(idTransformation, idStep, i, "return_value_default",
            returnValueDefault[i]);
        rep.saveStepAttribute(idTransformation, idStep, i, "return_value_type",
            ValueMeta.getTypeDesc(returnValueDefaultType[i]));
      }
    } catch (Exception e) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDBLookupMeta.Exception.UnableToSaveStepInfoToRepository") + idStep, e);
    }
  }

  /**
   * {@inheritDoc}
   * 
   * @throws KettleStepException
   */
  @Override
  public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info,
      StepMeta nextStep, VariableSpace space) throws KettleStepException {
    if (info==null || info[0] == null) {
      for (int i = 0; i < getReturnValueNewName().length; i++) {
        try {
          final ValueMetaInterface v =
              ValueMetaFactory.createValueMeta(getReturnValueNewName()[i],
                  getReturnValueDefaultType()[i]);
          v.setOrigin(origin);
          r.addValueMeta(v);
        } catch (Exception e) {
          throw new KettleStepException(e);
        }
      }
    } else {
      for (int i = 0; i < returnValueNewName.length; i++) {
        final ValueMetaInterface v = info[0].searchValueMeta(returnValueField[i]);
        if (v != null) {
          final ValueMetaInterface copy = v.clone();
          copy.setName(returnValueNewName[i]);
          copy.setOrigin(origin);
          r.addValueMeta(copy);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   * 
   * @throws KettleXMLException
   */
  @Override
  public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
      throws KettleXMLException {
    readData(stepnode);
  }

  /**
   * Sets the default values.
   */
  public void setDefault() {}

  /**
   * {@inheritDoc}
   */
  @Override
  public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta,
      RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info) {}

  /**
   * Get the Step dialog, needs for configure the step.
   * 
   * @param shell the shell.
   * @param meta the associated base step metadata.
   * @param transMeta the associated transformation metadata.
   * @param name the step name
   * @return The appropriate StepDialogInterface class.
   */
  public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta,
      String name) {
    return new MongoDBLookupDialog(shell, (BaseStepMeta) meta, transMeta, name);
  }

  /**
   * Get the executing step, needed by Trans to launch a step.
   * 
   * @param stepMeta The step info.
   * @param stepDataInterface the step data interface linked to this step. Here the step can store
   *        temporary data, database connections, etc.
   * @param cnr The copy nr to get.
   * @param transMeta The transformation info.
   * @param disp The launching transformation.
   * @return The appropriate StepInterface class.
   */
  public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
      TransMeta transMeta, Trans disp) {
    return new MongoDBLookup(stepMeta, stepDataInterface, cnr, transMeta, disp);
  }

  /**
   * Get a new instance of the appropriate data class. This data class implements the
   * StepDataInterface. It basically contains the persisting data that needs to live on, even if a
   * worker thread is terminated.
   * 
   * @return The appropriate StepDataInterface class.
   */
  public StepDataInterface getStepData() {
    return new MongoDBLookupData();
  }

  public void allocate(int nrkeys, int nrvalues) {
    streamKeyField1 = new String[nrkeys];
    collectionKeyField = new String[nrkeys];
    keyCondition = new String[nrkeys];
    returnValueField = new String[nrvalues];
    returnValueNewName = new String[nrvalues];
    returnValueDefault = new String[nrvalues];
    returnValueDefaultType = new int[nrvalues];
  }

  public Object clone() {
    final MongoDBLookupMeta retval = (MongoDBLookupMeta) super.clone();

    final int nrkeys = streamKeyField1 == null ? 0 : streamKeyField1.length;
    final int nrvalues = returnValueField == null ? 0 : returnValueField.length;

    retval.allocate(nrkeys, nrvalues);

    for (int i = 0; i < nrkeys; i++) {
      retval.streamKeyField1[i] = streamKeyField1[i];
      retval.collectionKeyField[i] = collectionKeyField[i];
      retval.keyCondition[i] = keyCondition[i];
    }

    for (int i = 0; i < nrvalues; i++) {
      retval.returnValueField[i] = returnValueField[i];
      retval.returnValueNewName[i] = returnValueNewName[i];
      retval.returnValueDefault[i] = returnValueDefault[i];
      retval.returnValueDefaultType[i] = returnValueDefaultType[i];
    }

    return retval;
  }
}
