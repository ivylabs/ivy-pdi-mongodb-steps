package com.ivyis.di.trans.steps.mongodb;

import java.util.List;
import java.util.Map;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
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
import org.w3c.dom.Node;

import com.ivyis.di.ui.trans.steps.mongodb.MongoDBMapReduceDialog;

/**
 * This class is responsible for implementing functionality regarding step meta. All Kettle steps
 * have an extension of this where private fields have been added with public accessors.
 * 
 * @author <a href="mailto:joel.latino@ivy-is.co.uk">Joel Latino</a>
 * @since 1.0.0
 */
@Step(id = "MongoDBMapReduce", name = "MongoDBMapReduce.Step.Name",
    description = "MongoDBMapReduce.Step.Description",
    categoryDescription = "MongoDBMapReduce.Step.Category",
    image = "com/ivyis/di/trans/steps/mongodb/MongoDBMapReduce.png",
    i18nPackageName = "com.ivyis.di.trans.steps.mongodb", casesUrl = "https://github.com/ivylabs",
    documentationUrl = "https://github.com/ivylabs", forumUrl = "https://github.com/ivylabs")
public class MongoDBInsertMeta extends MongoDBMeta implements StepMetaInterface {

  /** for i18n purposes. **/
  protected static final Class<?> PKG = MongoDBInsertMeta.class;

  /** Number records to insert for each batch. */
  private String batchInsertNum;

  /** Truncate MongoDB collection. */
  private boolean truncateCollection;

  /** JSON field. */
  private String jsonField;

  /** MongoDB write concern. */
  private String writeConcern;

  public String getBatchInsertNum() {
    return batchInsertNum;
  }

  public void setBatchInsertNum(String batchInsertNum) {
    this.batchInsertNum = batchInsertNum;
  }

  public boolean isTruncateCollection() {
    return truncateCollection;
  }

  public void setTruncateCollection(boolean truncateCollection) {
    this.truncateCollection = truncateCollection;
  }

  public String getJsonField() {
    return jsonField;
  }

  public void setJsonField(String jsonField) {
    this.jsonField = jsonField;
  }

  public String getWriteConcern() {
    return writeConcern;
  }

  public void setWriteConcern(String writeConcern) {
    this.writeConcern = writeConcern;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getXML() {
    final StringBuilder retval = new StringBuilder();
    retval.append("    " + XMLHandler.addTagValue("hostname", hostname));
    retval.append("    " + XMLHandler.addTagValue("port", port));
    retval.append("    " + XMLHandler.addTagValue("username", username));
    retval.append("    " + XMLHandler.addTagValue("password", password));
    retval.append("    "
        + XMLHandler.addTagValue("databaseName", databaseName));
    retval.append("    "
        + XMLHandler.addTagValue("collectionName", collectionName));
    retval.append("    "
        + XMLHandler.addTagValue("batchInsertNum", batchInsertNum));
    retval.append("    "
        + XMLHandler.addTagValue("truncateCollection",
            truncateCollection));
    retval.append("    " + XMLHandler.addTagValue("jsonField", jsonField));
    retval.append("    "
        + XMLHandler.addTagValue("writeConcern", writeConcern));

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
      hostname = XMLHandler.getTagValue(stepnode, "hostname");
      port = XMLHandler.getTagValue(stepnode, "port");
      username = XMLHandler.getTagValue(stepnode, "username");
      password = XMLHandler.getTagValue(stepnode, "password");
      databaseName = XMLHandler.getTagValue(stepnode, "databaseName");
      collectionName = XMLHandler.getTagValue(stepnode, "collectionName");
      batchInsertNum = XMLHandler.getTagValue(stepnode, "batchInsertNum");
      writeConcern = XMLHandler.getTagValue(stepnode, "writeConcern");
      truncateCollection = "Y".equalsIgnoreCase(XMLHandler.getTagValue(
          stepnode, "truncateCollection"));
      jsonField = XMLHandler.getTagValue(stepnode, "jsonField");

    } catch (Exception e) {
      throw new KettleXMLException(
          BaseMessages
              .getString(PKG,
                  "MongoDBMapReduceMeta.Exception.UnexpectedErrorInReadingStepInfo"),
          e);
    }
  }

  /**
   * {@inheritDoc}
   * 
   * @throws KettleException
   */
  @Override
  public void readRep(Repository rep, ObjectId idStep,
      List<DatabaseMeta> databases, Map<String, Counter> counters)
      throws KettleException {
    try {
      hostname = rep.getStepAttributeString(idStep, "hostname");
      port = rep.getStepAttributeString(idStep, "port");
      username = rep.getStepAttributeString(idStep, "username");
      password = rep.getStepAttributeString(idStep, "password");
      databaseName = rep.getStepAttributeString(idStep, "databaseName");
      collectionName = rep.getStepAttributeString(idStep,
          "collectionName");
      batchInsertNum = rep.getStepAttributeString(idStep,
          "batchInsertNum");
      writeConcern = rep.getStepAttributeString(idStep, "writeConcern");
      truncateCollection = Boolean.parseBoolean(rep
          .getStepAttributeString(idStep, "truncateCollection"));
      jsonField = rep.getStepAttributeString(idStep, "jsonField");

    } catch (Exception e) {
      throw new KettleException(
          BaseMessages
              .getString(PKG,
                  "MongoDBMapReduceMeta.Exception.UnexpectedErrorInReadingStepInfo"),
          e);
    }
  }

  /**
   * {@inheritDoc}
   * 
   * @throws KettleException
   */
  @Override
  public void saveRep(Repository rep, ObjectId idTransformation,
      ObjectId idStep) throws KettleException {
    try {
      rep.saveStepAttribute(idTransformation, idStep, "hostname",
          hostname);
      rep.saveStepAttribute(idTransformation, idStep, "port", port);
      rep.saveStepAttribute(idTransformation, idStep, "username",
          username);
      rep.saveStepAttribute(idTransformation, idStep, "password",
          password);
      rep.saveStepAttribute(idTransformation, idStep, "databaseName",
          databaseName);
      rep.saveStepAttribute(idTransformation, idStep, "collectionName",
          collectionName);
      rep.saveStepAttribute(idTransformation, idStep, "batchInsertNum",
          batchInsertNum);
      rep.saveStepAttribute(idTransformation, idStep, "writeConcern",
          writeConcern);
      rep.saveStepAttribute(idTransformation, idStep,
          "truncateCollection", truncateCollection);
      rep.saveStepAttribute(idTransformation, idStep, "jsonField",
          jsonField);

    } catch (Exception e) {
      throw new KettleException(
          BaseMessages.getString(PKG,
              "MongoDBMapReduceMeta.Exception.UnableToSaveStepInfoToRepository")
              + idStep, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void getFields(RowMetaInterface r, String origin,
      RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {}

  /**
   * {@inheritDoc}
   */
  @Override
  public Object clone() {
    return super.clone();
  }

  /**
   * {@inheritDoc}
   * 
   * @throws KettleXMLException
   */
  @Override
  public void loadXML(Node stepnode, List<DatabaseMeta> databases,
      Map<String, Counter> counters) throws KettleXMLException {
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
  public void check(List<CheckResultInterface> remarks, TransMeta transmeta,
      StepMeta stepMeta, RowMetaInterface prev, String[] input,
      String[] output, RowMetaInterface info) {}

  /**
   * Get the Step dialog, needs for configure the step.
   * 
   * @param shell the shell.
   * @param meta the associated base step metadata.
   * @param transMeta the associated transformation metadata.
   * @param name the step name
   * @return The appropriate StepDialogInterface class.
   */
  public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta,
      TransMeta transMeta, String name) {
    return new MongoDBMapReduceDialog(shell, (BaseStepMeta) meta,
        transMeta, name);
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
  public StepInterface getStep(StepMeta stepMeta,
      StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans disp) {
    return new MongoDBMapReduce(stepMeta, stepDataInterface, cnr,
        transMeta, disp);
  }

  /**
   * Get a new instance of the appropriate data class. This data class implements the
   * StepDataInterface. It basically contains the persisting data that needs to live on, even if a
   * worker thread is terminated.
   * 
   * @return The appropriate StepDataInterface class.
   */
  public StepDataInterface getStepData() {
    return new MongoDBMapReduceData();
  }

}
