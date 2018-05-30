package com.ivyis.di.trans.steps.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResultInterface;
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

import com.ivyis.di.trans.steps.mongodb.wrapper.field.MongoField;
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
public class MongoDBMapReduceMeta extends MongoDBMeta implements
    StepMetaInterface {

  /** for i18n purposes. **/
  protected static final Class<?> PKG = MongoDBMapReduceMeta.class;

  /** JavaScript Map function. */
  private String mapFunction;

  /** JavaScript Reduce function. */
  private String reduceFunction;

  /** Output in JSON field. */
  private boolean outputAsJson;

  /** JSON field. */
  private String jsonField;

  /** Output fields. */
  private List<MongoField> fields;

  public String getMapFunction() {
    return mapFunction;
  }

  public void setMapFunction(String mapFunction) {
    this.mapFunction = mapFunction;
  }

  public String getReduceFunction() {
    return reduceFunction;
  }

  public void setReduceFunction(String reduceFunction) {
    this.reduceFunction = reduceFunction;
  }

  public boolean isOutputAsJson() {
    return outputAsJson;
  }

  public void setOutputAsJson(boolean outputAsJson) {
    this.outputAsJson = outputAsJson;
  }

  public String getJsonField() {
    return jsonField;
  }

  public void setJsonField(String jsonField) {
    this.jsonField = jsonField;
  }

  public List<MongoField> getFields() {
    return fields;
  }

  public void setFields(List<MongoField> fields) {
    this.fields = fields;
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
    retval.append("    " + XMLHandler.addTagValue("mapFunction", mapFunction));
    retval.append("    " + XMLHandler.addTagValue("reduceFunction", reduceFunction));
    retval.append("    " + XMLHandler.addTagValue("outputAsJson", outputAsJson));
    retval.append("    " + XMLHandler.addTagValue("jsonField", jsonField));

    if (fields != null && fields.size() > 0) {
      retval.append("\n    ").append(XMLHandler.openTag("mongo_fields"));

      for (MongoField f : fields) {
        retval.append("\n      ").append(XMLHandler.openTag("mongo_field"));
        retval.append("\n        ").append(XMLHandler.addTagValue("field_name", f.mFieldName));
        retval.append("\n        ").append(XMLHandler.addTagValue("field_path", f.mFieldPath));
        retval.append("\n        ").append(XMLHandler.addTagValue("field_type", f.mKettleType));
        retval.append("\n      ").append(XMLHandler.closeTag("mongo_field"));
      }

      retval.append("\n    ").append(XMLHandler.closeTag("mongo_fields"));
    }

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
      username = XMLHandler.getTagValue(stepnode, "username");
      password = Encr.decryptPasswordOptionallyEncrypted(XMLHandler.getTagValue(stepnode, "password"));
      authDb = XMLHandler.getTagValue(stepnode, "auth_db");
      authMechanism = XMLHandler.getTagValue(stepnode, "auth_mechanism");

      databaseName = XMLHandler.getTagValue(stepnode, "databaseName");
      collectionName = XMLHandler.getTagValue(stepnode, "collectionName");
      mapFunction = XMLHandler.getTagValue(stepnode, "mapFunction");
      reduceFunction = XMLHandler.getTagValue(stepnode, "reduceFunction");
      outputAsJson = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "outputAsJson"));
      jsonField = XMLHandler.getTagValue(stepnode, "jsonField");

      final Node mongoFields = XMLHandler.getSubNode(stepnode, "mongo_fields");
      if (mongoFields != null && XMLHandler.countNodes(mongoFields, "mongo_field") > 0) {
        final int nrfields = XMLHandler.countNodes(mongoFields, "mongo_field");

        fields = new ArrayList<MongoField>();
        for (int i = 0; i < nrfields; i++) {
          final Node fieldNode = XMLHandler.getSubNodeByNr(mongoFields, "mongo_field", i);

          final MongoField newField = new MongoField();
          newField.mFieldName = XMLHandler.getTagValue(fieldNode, "field_name");
          newField.mFieldPath = XMLHandler.getTagValue(fieldNode, "field_path");
          newField.mKettleType = XMLHandler.getTagValue(fieldNode, "field_type");

          fields.add(newField);
        }
      }

    } catch (Exception e) {
      throw new KettleXMLException(BaseMessages.getString(PKG,
          "MongoDBMapReduceMeta.Exception.UnexpectedErrorInReadingStepInfo"), e);
    }
  }

  /**
   * {@inheritDoc}
   * 
   * @throws KettleException
   */
  @Override
  public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases,
      Map<String, Counter> counters) throws KettleException {
    try {
      servers = rep.getStepAttributeString(idStep, "servers");
      username = rep.getStepAttributeString(idStep, "username");
      password = Encr.decryptPasswordOptionallyEncrypted( rep.getStepAttributeString(idStep, "password") );
      authDb = rep.getStepAttributeString(idStep, "auth_db");
      authMechanism = rep.getStepAttributeString(idStep, "auth_mechanism");

      databaseName = rep.getStepAttributeString(idStep, "databaseName");
      collectionName = rep.getStepAttributeString(idStep, "collectionName");
      mapFunction = rep.getStepAttributeString(idStep, "mapFunction");
      reduceFunction = rep.getStepAttributeString(idStep, "reduceFunction");
      outputAsJson = Boolean.parseBoolean(rep.getStepAttributeString(idStep, "outputAsJson"));
      jsonField = rep.getStepAttributeString(idStep, "jsonField");

      final int nrfields = rep.countNrStepAttributes(idStep, "field_name");
      if (nrfields > 0) {
        fields = new ArrayList<MongoField>();

        for (int i = 0; i < nrfields; i++) {
          final MongoField newField = new MongoField();
          newField.mFieldName = rep.getStepAttributeString(idStep, i, "field_name");
          newField.mFieldPath = rep.getStepAttributeString(idStep, i, "field_path");
          newField.mKettleType = rep.getStepAttributeString(idStep, i, "field_type");
          fields.add(newField);
        }
      }
    } catch (Exception e) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDBMapReduceMeta.Exception.UnexpectedErrorInReadingStepInfo"), e);
    }
  }

  /**
   * {@inheritDoc}
   * 
   * @throws KettleException
   */
  @Override
  public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep)
      throws KettleException {
    try {
      rep.saveStepAttribute(idTransformation, idStep, "servers", servers);
      rep.saveStepAttribute(idTransformation, idStep, "username", username);
      rep.saveStepAttribute(idTransformation, idStep, "password", password);
      rep.saveStepAttribute(idTransformation, idStep, "auth_db", authDb);
      rep.saveStepAttribute(idTransformation, idStep, "auth_mechanism", authMechanism);

      rep.saveStepAttribute(idTransformation, idStep, "databaseName", databaseName);
      rep.saveStepAttribute(idTransformation, idStep, "collectionName", collectionName);
      rep.saveStepAttribute(idTransformation, idStep, "mapFunction", mapFunction);
      rep.saveStepAttribute(idTransformation, idStep, "reduceFunction", reduceFunction);
      rep.saveStepAttribute(idTransformation, idStep, "outputAsJson", outputAsJson);
      rep.saveStepAttribute(idTransformation, idStep, "jsonField", jsonField);

      if (fields != null && fields.size() > 0) {
        for (int i = 0; i < fields.size(); i++) {
          final MongoField f = fields.get(i);
          rep.saveStepAttribute(idTransformation, idStep, i, "field_name", f.mFieldName);
          rep.saveStepAttribute(idTransformation, idStep, i, "field_path", f.mFieldPath);
          rep.saveStepAttribute(idTransformation, idStep, i, "field_type", f.mKettleType);
        }
      }

    } catch (Exception e) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDBMapReduceMeta.Exception.UnableToSaveStepInfoToRepository") + idStep, e);
    }
  }

  /**
   * {@inheritDoc}
   */

  @Override
  public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository,
                        IMetaStore metaStore ) throws KettleStepException {
    try {
      if ( outputAsJson || fields == null || fields.size() == 0 ) {
        final ValueMetaInterface jsonValueMeta =
          new ValueMeta( jsonField, ValueMetaInterface.TYPE_STRING );
        jsonValueMeta.setOrigin( origin );
        r.addValueMeta( jsonValueMeta );
      } else {
        for ( MongoField f : fields ) {
          final ValueMetaInterface vm = ValueMetaFactory.createValueMeta( f.mFieldName, ValueMeta.getType( f.mKettleType ) );
          vm.setOrigin( origin );

          if ( f.mIndexedVals != null ) {
            vm.setIndex( f.mIndexedVals.toArray() ); // indexed values
          }
          r.addValueMeta( vm );
        }
      }
    } catch(Exception e) {
      throw new KettleStepException( "Unable to get fields from lookup", e );
    }
  }


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
  public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
      throws KettleXMLException {
    readData(stepnode);
  }

  /**
   * Sets the default values.
   */
  public void setDefault() {}


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
    return new MongoDBMapReduceDialog(shell, (BaseStepMeta) meta, transMeta, name);
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
    return new MongoDBMapReduce(stepMeta, stepDataInterface, cnr, transMeta, disp);
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
