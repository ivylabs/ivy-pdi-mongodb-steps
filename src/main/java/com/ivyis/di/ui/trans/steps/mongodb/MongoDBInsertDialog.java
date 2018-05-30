package com.ivyis.di.ui.trans.steps.mongodb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.steps.script.ScriptAddedFunctions;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.ShowBrowserDialog;
import org.pentaho.di.ui.core.dialog.ShowMessageDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.LabelTextVar;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.util.ImageUtil;

import com.ivyis.di.trans.steps.mongodb.MongoClientWrapper;
import com.ivyis.di.trans.steps.mongodb.MongoDBMapReduceData;
import com.ivyis.di.trans.steps.mongodb.MongoDBMapReduceMeta;
import com.ivyis.di.trans.steps.mongodb.wrapper.field.MongoField;

/**
 * This class is responsible for the UI in Spoon of MongoDB map and reduce step.
 * 
 * @author <a href="mailto:joel.latino@ivy-is.co.uk">Joel Latino</a>
 * @since 1.0.0
 */
public class MongoDBInsertDialog extends BaseStepDialog implements StepDialogInterface {

  /** for i18n purposes. **/
  private static final Class<?> PKG = MongoDBInsertDialog.class;

  private CTabFolder wTabFolder;
  private CTabItem wMongoConfigTab;
  private CTabItem wMongoMapTab;
  private CTabItem wMongoReduceTab;
  private CTabItem wMongoFieldsTab;

  private TextVar wServers;
  private TextVar wAuthUser;
  private LabelTextVar wAuthPass;
  private TextVar wAuthDb;
  private TextVar wAuthMechanism;

  private CCombo wDbName;
  private Button wgetDbsBut;
  private CCombo wCollection;
  private Button wgetCollectionsBut;
  private StyledTextComp wMapFuncScript;
  private StyledTextComp wReduceFuncScript;
  private Button wOutputAsJson;
  private TextVar wJsonField;
  private TableView wfieldsView;

  private MongoDBMapReduceMeta input;
  private Shell parent;

  public MongoDBInsertDialog(Shell parent, BaseStepMeta in, TransMeta transMeta, String sname) {
    super(parent, in, transMeta, sname);
    this.input = (MongoDBMapReduceMeta) in;
  }

  /**
   * Opens a step dialog window.
   * 
   * @return the (potentially new) name of the step
   */
  public String open() {
    this.parent = getParent();
    final Display display = parent.getDisplay();

    this.shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, this.input);

    final ModifyListener lsMod = new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        input.setChanged();
      }
    };
    backupChanged = input.hasChanged();

    final FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "MongoDBMapReduceDialog.Shell.Title"));

    final int middle = props.getMiddlePct();
    final int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label(shell, SWT.RIGHT);
    wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName"));
    props.setLook(wlStepname);
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment(0, 0);
    fdlStepname.right = new FormAttachment(middle, -margin);
    fdlStepname.top = new FormAttachment(0, margin);
    wlStepname.setLayoutData(fdlStepname);

    wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wStepname.setText(stepname);
    props.setLook(wStepname);
    wStepname.addModifyListener(lsMod);
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment(middle, 0);
    fdStepname.top = new FormAttachment(0, margin);
    fdStepname.right = new FormAttachment(100, 0);
    wStepname.setLayoutData(fdStepname);

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // MongoDB config TAB
    wMongoConfigTab = new CTabItem(wTabFolder, SWT.NONE);
    wMongoConfigTab.setText(BaseMessages.getString(PKG,
        "MongoDBMapReduceDialog.MongoConfigTab.CTabItem.Title"));

    final FormLayout mainOptionsLayout = new FormLayout();
    mainOptionsLayout.marginWidth = Const.MARGIN;
    mainOptionsLayout.marginHeight = Const.MARGIN;

    final Composite wMongoConfigComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wMongoConfigComp);
    wMongoConfigComp.setLayout(mainOptionsLayout);

    // Servers line...
    final Label wlServers = new Label(wMongoConfigComp, SWT.RIGHT);
    wlServers.setText("Servers");
    props.setLook(wlServers);
    final FormData fdlServers = new FormData();
    fdlServers.left = new FormAttachment(0, 0);
    fdlServers.right = new FormAttachment(middle, -margin);
    fdlServers.top = new FormAttachment(0, margin);
    wlServers.setLayoutData(fdlServers);

    wServers = new TextVar(transMeta, wMongoConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook( wServers );
    wServers.addModifyListener(lsMod);
    final FormData fdHostname = new FormData();
    fdHostname.left = new FormAttachment(middle, 0);
    fdHostname.top = new FormAttachment(0, margin);
    fdHostname.right = new FormAttachment(100, 0);
    wServers.setLayoutData(fdHostname);

    // AuthUser field
    final Label wlAuthUser = new Label(wMongoConfigComp, SWT.RIGHT);
    wlAuthUser.setText(BaseMessages.getString(PKG, "MongoDBMapReduceDialog.AuthUser.Label"));
    props.setLook(wlAuthUser);
    final FormData fdlAuthUser = new FormData();
    fdlAuthUser.left = new FormAttachment(0, 0);
    fdlAuthUser.top = new FormAttachment(wServers, margin * 2);
    fdlAuthUser.right = new FormAttachment(middle, -margin);
    wlAuthUser.setLayoutData(fdlAuthUser);

    wAuthUser = new TextVar(this.transMeta, wMongoConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wAuthUser.setEditable(true);
    props.setLook(wAuthUser);
    final FormData fdAuthUser = new FormData();
    fdAuthUser.left = new FormAttachment(middle, 0);
    fdAuthUser.top = new FormAttachment(wServers, margin * 2);
    fdAuthUser.right = new FormAttachment(100, 0);
    wAuthUser.setLayoutData(fdAuthUser);

    // Password field
    wAuthPass = new LabelTextVar(transMeta, wMongoConfigComp, BaseMessages.getString(PKG,
      "MongoDBMapReduceDialog.AuthPass.Label"), BaseMessages.getString(PKG,
      "MongoDBMapReduceDialog.AuthPass.Tooltip"));
    props.setLook(wAuthPass);
    wAuthPass.setEchoChar('*');
    wAuthPass.addModifyListener(lsMod);
    final FormData fdAuthPass = new FormData();
    fdAuthPass.left = new FormAttachment(0, -margin);
    fdAuthPass.top = new FormAttachment(wAuthUser, margin);
    fdAuthPass.right = new FormAttachment(100, 0);
    wAuthPass.setLayoutData(fdAuthPass);

    // OK, if the password contains a variable, we don't want to have the password hidden...
    wAuthPass.getTextWidget().addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        checkPasswordVisible();
      }
    });


    // AuthDb field
    final Label wlAuthDb = new Label(wMongoConfigComp, SWT.RIGHT);
    wlAuthDb.setText("Authentication database");
    props.setLook(wlAuthDb);
    final FormData fdlAuthDb = new FormData();
    fdlAuthDb.left = new FormAttachment(0, 0);
    fdlAuthDb.top = new FormAttachment(wAuthPass, margin * 2);
    fdlAuthDb.right = new FormAttachment(middle, -margin);
    wlAuthDb.setLayoutData(fdlAuthDb);

    wAuthDb = new TextVar(this.transMeta, wMongoConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wAuthDb.setEditable(true);
    props.setLook(wAuthDb);
    final FormData fdAuthDb = new FormData();
    fdAuthDb.left = new FormAttachment(middle, 0);
    fdAuthDb.top = new FormAttachment(wAuthPass, margin * 2);
    fdAuthDb.right = new FormAttachment(100, 0);
    wAuthDb.setLayoutData(fdAuthDb);

    // AuthMechanism field
    final Label wlAuthMechanism = new Label(wMongoConfigComp, SWT.RIGHT);
    wlAuthMechanism.setText("Authentication Mechanism");
    props.setLook(wlAuthMechanism);
    final FormData fdlAuthMechanism = new FormData();
    fdlAuthMechanism.left = new FormAttachment(0, 0);
    fdlAuthMechanism.top = new FormAttachment(wAuthDb, margin * 2);
    fdlAuthMechanism.right = new FormAttachment(middle, -margin);
    wlAuthMechanism.setLayoutData(fdlAuthMechanism);

    wAuthMechanism = new TextVar(this.transMeta, wMongoConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wAuthMechanism.setEditable(true);
    props.setLook(wAuthMechanism);
    final FormData fdAuthMechanism = new FormData();
    fdAuthMechanism.left = new FormAttachment(middle, 0);
    fdAuthMechanism.top = new FormAttachment(wAuthDb, margin * 2);
    fdAuthMechanism.right = new FormAttachment(100, 0);
    wAuthMechanism.setLayoutData(fdAuthMechanism);

    // DbName input ...
    final Label wlDbName = new Label(wMongoConfigComp, SWT.RIGHT);
    wlDbName.setText(BaseMessages.getString(PKG, "MongoDBMapReduceDialog.DbName.Label"));
    props.setLook(wlDbName);
    final FormData fdlDbName = new FormData();
    fdlDbName.left = new FormAttachment(0, 0);
    fdlDbName.right = new FormAttachment(middle, -margin);
    fdlDbName.top = new FormAttachment(wAuthMechanism, margin);
    wlDbName.setLayoutData(fdlDbName);

    wgetDbsBut = new Button(wMongoConfigComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wgetDbsBut);
    wgetDbsBut.setText(BaseMessages.getString(PKG, "MongoDBMapReduceDialog.DbName.Button"));
    final FormData fd = new FormData();
    fd.top = new FormAttachment(wAuthMechanism, margin);
    fd.right = new FormAttachment(100, 0);
    wgetDbsBut.setLayoutData(fd);

    wgetDbsBut.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        setupDBNames();
      }
    });

    wDbName = new CCombo(wMongoConfigComp, SWT.BORDER);
    props.setLook(wDbName);
    wDbName.addModifyListener(lsMod);
    final FormData fdDbName = new FormData();
    fdDbName.left = new FormAttachment(middle, 0);
    fdDbName.top = new FormAttachment(wAuthMechanism, margin);
    fdDbName.right = new FormAttachment(wgetDbsBut, 0);
    wDbName.setLayoutData(fdDbName);
    wDbName.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        input.setChanged();
        wDbName.setToolTipText(transMeta.environmentSubstitute(wDbName
            .getText()));
      }
    });

    // Collection input ...
    final Label wlCollection = new Label(wMongoConfigComp, SWT.RIGHT);
    wlCollection.setText(BaseMessages.getString(PKG, "MongoDBMapReduceDialog.Collection.Label"));
    props.setLook(wlCollection);
    final FormData fdlCollection = new FormData();
    fdlCollection.left = new FormAttachment(0, 0);
    fdlCollection.right = new FormAttachment(middle, -margin);
    fdlCollection.top = new FormAttachment(wDbName, margin);
    wlCollection.setLayoutData(fdlCollection);

    wgetCollectionsBut = new Button(wMongoConfigComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wgetCollectionsBut);
    wgetCollectionsBut.setText(BaseMessages.getString(PKG,
        "MongoDBMapReduceDialog.GetCollections.Button"));
    final FormData fdGetCollectionsBut = new FormData();
    fdGetCollectionsBut.right = new FormAttachment(100, 0);
    fdGetCollectionsBut.top = new FormAttachment(wDbName, 0);
    wgetCollectionsBut.setLayoutData(fdGetCollectionsBut);
    wgetCollectionsBut.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        setupCollectionNamesForDB();
      }
    });

    wCollection = new CCombo(wMongoConfigComp, SWT.BORDER);
    props.setLook(wCollection);
    wCollection.addModifyListener(lsMod);
    final FormData fdCollection = new FormData();
    fdCollection.left = new FormAttachment(middle, 0);
    fdCollection.top = new FormAttachment(wDbName, margin);
    fdCollection.right = new FormAttachment(wgetCollectionsBut, 0);
    wCollection.setLayoutData(fdCollection);

    wMongoConfigComp.layout();
    wMongoConfigTab.setControl(wMongoConfigComp);
    // END OF Mongo Config Tab

    final FormData fdMongoConfigTab = new FormData();
    fdMongoConfigTab.left = new FormAttachment(0, 0);
    fdMongoConfigTab.top = new FormAttachment(wStepname, margin);
    fdMongoConfigTab.right = new FormAttachment(100, 0);
    fdMongoConfigTab.bottom = new FormAttachment(100, -50);
    wTabFolder.setLayoutData(fdMongoConfigTab);

    // START Mongo Map TAB
    wMongoMapTab = new CTabItem(wTabFolder, SWT.NONE);
    wMongoMapTab.setText(BaseMessages.getString(PKG,
        "MongoDBMapReduceDialog.MongoMapTab.CTabItem.Title"));

    final Composite wMongoMapComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wMongoMapComp);

    final FormLayout propsCompLayout = new FormLayout();
    propsCompLayout.marginWidth = Const.FORM_MARGIN;
    propsCompLayout.marginHeight = Const.FORM_MARGIN;
    wMongoMapComp.setLayout(propsCompLayout);

    // Map Func ...
    final Label wlMapFuncScript = new Label(wMongoMapComp, SWT.NONE);
    wlMapFuncScript.setText(BaseMessages.getString(PKG, "MongoDBMapReduceDialog.MapFunc.Label"));
    props.setLook(wlMapFuncScript);
    final FormData fdlMapFuncScript = new FormData();
    fdlMapFuncScript.left = new FormAttachment(0, 0);
    fdlMapFuncScript.right = new FormAttachment(100, -margin);
    fdlMapFuncScript.top = new FormAttachment(0, margin);
    wlMapFuncScript.setLayoutData(fdlMapFuncScript);

    wMapFuncScript =
        new StyledTextComp(transMeta, wMongoMapComp, SWT.MULTI | SWT.LEFT | SWT.BORDER
            | SWT.H_SCROLL | SWT.V_SCROLL, "");
    props.setLook(wMapFuncScript, PropsUI.WIDGET_STYLE_FIXED);
    wMapFuncScript.addModifyListener(lsMod);
    wMapFuncScript.addLineStyleListener(new ScriptHighlight(ScriptAddedFunctions.jsFunctionList));

    final FormData fdJsonQuery = new FormData();
    fdJsonQuery.left = new FormAttachment(0, 0);
    fdJsonQuery.top = new FormAttachment(wlMapFuncScript, margin);
    fdJsonQuery.right = new FormAttachment(100, -2 * margin);
    fdJsonQuery.bottom = new FormAttachment(100, -margin);
    wMapFuncScript.setLayoutData(fdJsonQuery);

    final FormData fdMongoMapComp = new FormData();
    fdMongoMapComp.left = new FormAttachment(0, 0);
    fdMongoMapComp.top = new FormAttachment(0, 0);
    fdMongoMapComp.right = new FormAttachment(100, 0);
    fdMongoMapComp.bottom = new FormAttachment(100, 0);
    wMongoMapComp.setLayoutData(fdMongoMapComp);

    wMongoMapComp.layout();
    wMongoMapTab.setControl(wMongoMapComp);

    final FormData fdwMongoMapTab = new FormData();
    fdwMongoMapTab.left = new FormAttachment(0, 0);
    fdwMongoMapTab.top = new FormAttachment(wStepname, margin);
    fdwMongoMapTab.right = new FormAttachment(100, 0);
    fdwMongoMapTab.bottom = new FormAttachment(100, -50);
    wTabFolder.setLayoutData(fdwMongoMapTab);

    // START Mongo Reduce TAB
    wMongoReduceTab = new CTabItem(wTabFolder, SWT.NONE);
    wMongoReduceTab.setText(BaseMessages.getString(PKG,
        "MongoDBMapReduceDialog.MongoReduceTab.CTabItem.Title"));

    final Composite wMongoReduceComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wMongoReduceComp);

    final FormLayout propsReduceCompLayout = new FormLayout();
    propsReduceCompLayout.marginWidth = Const.FORM_MARGIN;
    propsReduceCompLayout.marginHeight = Const.FORM_MARGIN;
    wMongoReduceComp.setLayout(propsReduceCompLayout);

    // JSON Query input ...
    //
    final Label wlReduceFuncScript = new Label(wMongoReduceComp, SWT.NONE);
    wlReduceFuncScript.setText(BaseMessages.getString(PKG,
        "MongoDBMapReduceDialog.ReduceFunc.Label"));
    props.setLook(wlReduceFuncScript);
    final FormData fdlReduceFuncScript = new FormData();
    fdlReduceFuncScript.left = new FormAttachment(0, 0);
    fdlReduceFuncScript.right = new FormAttachment(100, -margin);
    fdlReduceFuncScript.top = new FormAttachment(0, margin);
    wlReduceFuncScript.setLayoutData(fdlReduceFuncScript);

    wReduceFuncScript =
        new StyledTextComp(transMeta, wMongoReduceComp, SWT.MULTI | SWT.LEFT | SWT.BORDER
            | SWT.H_SCROLL | SWT.V_SCROLL, "");
    props.setLook(wReduceFuncScript, PropsUI.WIDGET_STYLE_FIXED);
    wReduceFuncScript.addModifyListener(lsMod);
    wReduceFuncScript
        .addLineStyleListener(new ScriptHighlight(ScriptAddedFunctions.jsFunctionList));

    final FormData fdReduceFunc = new FormData();
    fdReduceFunc.left = new FormAttachment(0, 0);
    fdReduceFunc.top = new FormAttachment(wlReduceFuncScript, margin);
    fdReduceFunc.right = new FormAttachment(100, -2 * margin);
    fdReduceFunc.bottom = new FormAttachment(100, -margin);
    wReduceFuncScript.setLayoutData(fdReduceFunc);

    final FormData fdMongoReduceComp = new FormData();
    fdMongoReduceComp.left = new FormAttachment(0, 0);
    fdMongoReduceComp.top = new FormAttachment(0, 0);
    fdMongoReduceComp.right = new FormAttachment(100, 0);
    fdMongoReduceComp.bottom = new FormAttachment(100, 0);
    wMongoReduceComp.setLayoutData(fdMongoReduceComp);

    wMongoReduceComp.layout();
    wMongoReduceTab.setControl(wMongoReduceComp);

    final FormData fdMongoReduceTab = new FormData();
    fdMongoReduceTab.left = new FormAttachment(0, 0);
    fdMongoReduceTab.top = new FormAttachment(0, margin);
    fdMongoReduceTab.right = new FormAttachment(100, 0);
    fdMongoReduceTab.bottom = new FormAttachment(100, -50);
    wTabFolder.setLayoutData(fdMongoReduceTab);

    // fields tab
    wMongoFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wMongoFieldsTab.setText(BaseMessages
        .getString(PKG, "MongoDBMapReduceDialog.FieldsTab.TabTitle"));
    final Composite wMongoFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wMongoFieldsComp);
    final FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;
    wMongoFieldsComp.setLayout(fieldsLayout);

    // Output as Json check box
    final Label outputJLab = new Label(wMongoFieldsComp, SWT.RIGHT);
    outputJLab.setText(BaseMessages.getString(PKG,
        "MongoDBMapReduceDialog.OutputJson.Label"));
    props.setLook(outputJLab);
    final FormData fdOutputJLab = new FormData();
    fdOutputJLab.top = new FormAttachment(0, 0);
    fdOutputJLab.left = new FormAttachment(0, 0);
    fdOutputJLab.right = new FormAttachment(middle, -margin);
    outputJLab.setLayoutData(fdOutputJLab);
    wOutputAsJson = new Button(wMongoFieldsComp, SWT.CHECK);
    props.setLook(wOutputAsJson);
    final FormData fdOutputAsJson = new FormData();
    fdOutputAsJson.top = new FormAttachment(0, 0);
    fdOutputAsJson.left = new FormAttachment(middle, 0);
    fdOutputAsJson.right = new FormAttachment(100, 0);
    wOutputAsJson.setLayoutData(fdOutputAsJson);
    wOutputAsJson.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        input.setChanged();
        wGet.setEnabled(!wOutputAsJson.getSelection());
        wfieldsView.setEnabled(!wOutputAsJson.getSelection());
        wJsonField.setEnabled(wOutputAsJson.getSelection());
      }
    });

    // JsonField input ...
    final Label wlJsonField = new Label(wMongoFieldsComp, SWT.RIGHT);
    wlJsonField.setText(BaseMessages.getString(PKG, "MongoDBMapReduceDialog.JsonField.Label"));
    props.setLook(wlJsonField);
    final FormData fdlJsonField = new FormData();
    fdlJsonField.left = new FormAttachment(0, 0);
    fdlJsonField.right = new FormAttachment(middle, -margin);
    fdlJsonField.top = new FormAttachment(wOutputAsJson, margin);
    wlJsonField.setLayoutData(fdlJsonField);
    wJsonField = new TextVar(transMeta, wMongoFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wJsonField);
    wJsonField.addModifyListener(lsMod);
    final FormData fdJsonField = new FormData();
    fdJsonField.left = new FormAttachment(middle, 0);
    fdJsonField.top = new FormAttachment(wOutputAsJson, margin);
    fdJsonField.right = new FormAttachment(100, 0);
    wJsonField.setLayoutData(fdJsonField);

    // get fields button
    wGet = new Button(wMongoFieldsComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "MongoDBMapReduceDialog.Button.GetFields"));
    props.setLook(wGet);
    final FormData fdGet = new FormData();
    fdGet.right = new FormAttachment(100, 0);
    fdGet.bottom = new FormAttachment(100, 0);
    wGet.setLayoutData(fdGet);
    wGet.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        // populate table from schema
        final MongoDBMapReduceMeta newMeta = (MongoDBMapReduceMeta) input.clone();
        getFields(newMeta);
      }
    });

    // fields stuff
    final ColumnInfo[] colinf =
        new ColumnInfo[] {
            new ColumnInfo(BaseMessages.getString(PKG, "MongoDBMapReduceDialog.Fields.FIELD_NAME"),
                ColumnInfo.COLUMN_TYPE_TEXT, false),
            new ColumnInfo(BaseMessages.getString(PKG, "MongoDBMapReduceDialog.Fields.FIELD_PATH"),
                ColumnInfo.COLUMN_TYPE_TEXT, false),
            new ColumnInfo(BaseMessages.getString(PKG, "MongoDBMapReduceDialog.Fields.FIELD_TYPE"),
                ColumnInfo.COLUMN_TYPE_CCOMBO, false)};
    colinf[2].setComboValues(ValueMeta.getTypes());

    wfieldsView =
        new TableView(transMeta, wMongoFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colinf, 1,
            lsMod, props);

    final FormData fdfieldsView = new FormData();
    fdfieldsView.top = new FormAttachment(wJsonField, margin * 2);
    fdfieldsView.bottom = new FormAttachment(wGet, -margin * 2);
    fdfieldsView.left = new FormAttachment(0, 0);
    fdfieldsView.right = new FormAttachment(100, 0);
    wfieldsView.setLayoutData(fdfieldsView);

    final FormData fdMongoFieldsComp = new FormData();
    fdMongoFieldsComp.left = new FormAttachment(0, 0);
    fdMongoFieldsComp.top = new FormAttachment(0, 0);
    fdMongoFieldsComp.right = new FormAttachment(100, 0);
    fdMongoFieldsComp.bottom = new FormAttachment(100, 0);
    wMongoFieldsComp.setLayoutData(fdMongoFieldsComp);

    wMongoFieldsComp.layout();
    wMongoFieldsTab.setControl(wMongoFieldsComp);

    // OK and cancel buttons
    wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    BaseStepDialog.positionBottomButtons(shell, new Button[] {wOK, wCancel}, margin, null);

    final Button button = new Button(shell, SWT.PUSH);
    button.setImage(ImageUtil.getImage(display, getClass(), "ivyis_logo.png"));
    button.setToolTipText("Ivy Information Systems");
    button.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        final ShowBrowserDialog sbd =
            new ShowBrowserDialog(shell, BaseMessages.getString(PKG,
                "ExportCmdLine.CommandLine.Title"),
                "<html><script>window.location=\"http://www.ivy-is.co.uk\"</script></html>");
        sbd.open();
      }
    });

    // Determine the largest button in the array
    Rectangle largest = null;
    button.pack(true);
    final Rectangle r = button.getBounds();
    if (largest == null || r.width > largest.width) {
      largest = r;
    }

    // Also, set the tooltip the same as the name if we don't have one...
    if (button.getToolTipText() == null) {
      button.setToolTipText(Const.replace(button.getText(), "&", ""));
    }

    // Make buttons a bit larger... (nicer)
    largest.width += 10;
    if ((largest.width % 2) == 1) {
      largest.width++;
    }

    BaseStepDialog.rightAlignButtons(new Button[] {button}, largest.width, margin, null);
    if (Const.isOSX()) {
      final List<TableView> tableViews = new ArrayList<TableView>();
      getTableViews(shell, tableViews);
      button.addSelectionListener(new SelectionAdapter() {
        public void widgetSelected(SelectionEvent e) {
          for (TableView view : tableViews) {
            view.applyOSXChanges();
          }
        }
      });
    }

    final FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wStepname, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(100, -50);
    fdTabFolder.bottom = new FormAttachment(wOK, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    wTabFolder.setSelection(0);

    final FormData fdSc = new FormData();
    fdSc.left = new FormAttachment(0, 0);
    fdSc.top = new FormAttachment(wStepname, margin);
    fdSc.right = new FormAttachment(100, 0);
    fdSc.bottom = new FormAttachment(100, -50);

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent(Event e) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent(Event e) {
        ok();
      }
    };

    wCancel.addListener(SWT.Selection, lsCancel);
    wOK.addListener(SWT.Selection, lsOK);

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };
    wStepname.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(new ShellAdapter() {
      @Override
      public void shellClosed(ShellEvent e) {
        cancel();
      }
    });

    // Set the shell size, based upon previous time...
    setSize();

    getData(input);
    input.setChanged(backupChanged);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return stepname;
  }

  /**
   * Gets the table views.
   * 
   * @param parentControl the parent control
   * @param tableViews the table views
   * @return the table views
   */
  private static final void getTableViews(Control parentControl, List<TableView> tableViews) {
    if (parentControl instanceof TableView) {
      tableViews.add((TableView) parentControl);
    } else {
      if (parentControl instanceof Composite) {
        final Control[] children = ((Composite) parentControl).getChildren();
        for (Control child : children) {
          getTableViews(child, tableViews);
        }
      } else {
        if (parentControl instanceof Shell) {
          final Control[] children = ((Shell) parentControl).getChildren();
          for (Control child : children) {
            getTableViews(child, tableViews);
          }

        }
      }
    }
  }

  /**
   * Checks the password visible.
   */
  private void checkPasswordVisible() {
    final String password = wAuthPass.getText();
    final List<String> list = new ArrayList<String>();
    StringUtil.getUsedVariables(password, list, true);
    if (list.size() == 0) {
      wAuthPass.setEchoChar('*');
    } else {
      String variableName = null;
      if (password.startsWith(StringUtil.UNIX_OPEN) && password.endsWith(StringUtil.UNIX_CLOSE)) {
        variableName =
            password.substring(StringUtil.UNIX_OPEN.length(), password.length()
                - StringUtil.UNIX_CLOSE.length());
      }
      if (password.startsWith(StringUtil.WINDOWS_OPEN)
          && password.endsWith(StringUtil.WINDOWS_CLOSE)) {
        variableName =
            password.substring(StringUtil.WINDOWS_OPEN.length(), password.length()
                - StringUtil.WINDOWS_CLOSE.length());
      }
      if (variableName != null && System.getProperty(variableName) != null) {
        wAuthPass.setEchoChar('\0');
      } else {
        wAuthPass.setEchoChar('*');
      }
    }
  }

  private void getFields(MongoDBMapReduceMeta meta) {
    if (!StringUtils.isEmpty( wServers.getText()) && !StringUtils.isEmpty(wDbName.getText())
        && !StringUtils.isEmpty(wCollection.getText())) {
      final EnterNumberDialog end =
          new EnterNumberDialog(shell, 100, BaseMessages.getString(PKG,
              "MongoDBMapReduceDialog.SampleDocuments.Title"), BaseMessages.getString(PKG,
              "MongoDBMapReduceDialog.SampleDocuments.Message"));
      final int samples = end.open();
      if (samples > 0) {
        try {
          getInfo(meta, false);

          if (!checkForUnresolved(
              meta,
              BaseMessages
                  .getString(PKG,
                      "MongoDBMapReduceDialog.Warning.Message."
                          + "MongoQueryContainsUnresolvedVarsFieldSubs.SamplingTitle"))) {
            return;
          }

          final boolean result = MongoDBMapReduceData.discoverFields(meta, transMeta, samples);

          if (!result) {
            new ErrorDialog(shell, stepname, BaseMessages.getString(PKG,
                "MongoDBMapReduceDialog.ErrorMessage.NoFieldsFound"), new KettleException(
                BaseMessages.getString(PKG, "MongoDBMapReduceDialog.ErrorMessage.NoFieldsFound")));
          } else {
            getData(meta);
          }
        } catch (KettleException e) {
          new ErrorDialog(shell, stepname, BaseMessages.getString(PKG,
              "MongoDBMapReduceDialog.ErrorMessage.ErrorDuringSampling"), e);
        }
      }
    } else {
      // pop up an error dialog

      String missingConDetails = "";
      if (StringUtils.isEmpty( wServers.getText())) {
        missingConDetails += " host name(s)";
      }
      if (StringUtils.isEmpty(wDbName.getText())) {
        missingConDetails += " database";
      }
      if (StringUtils.isEmpty(wCollection.getText())) {
        missingConDetails += " collection";
      }

      final ShowMessageDialog smd =
          new ShowMessageDialog(shell, SWT.ICON_WARNING | SWT.OK, BaseMessages.getString(PKG,
              "MongoDBMapReduceDialog.ErrorMessage.MissingConnectionDetails.Title"),
              BaseMessages
                  .getString(PKG, "MongoDBMapReduceDialog.ErrorMessage.MissingConnectionDetails",
                      missingConDetails));
      smd.open();
    }
  }

  private boolean checkForUnresolved(MongoDBMapReduceMeta meta, String title) {
    final String mapFunction = transMeta.environmentSubstitute(meta.getMapFunction());
    final boolean mapFunctionNotOk = (mapFunction.contains("${") || mapFunction.contains("?{"));
    if (mapFunctionNotOk) {
      final ShowMessageDialog smd =
          new ShowMessageDialog(shell, SWT.ICON_WARNING | SWT.OK, title,
              BaseMessages.getString(PKG,
                  "MongoDBMapReduceDialog.Warning.Message."
                      + "MongoMapContainsUnresolvedVarsFieldSubs"));
      smd.open();
    }

    final String reduceFunction = transMeta.environmentSubstitute(meta.getReduceFunction());
    final boolean reduceFunctionNotOk =
        (reduceFunction.contains("${") || reduceFunction.contains("?{"));
    if (reduceFunctionNotOk) {
      final ShowMessageDialog smd =
          new ShowMessageDialog(shell, SWT.ICON_WARNING | SWT.OK, title, BaseMessages.getString(
              PKG,
              "MongoDBMapReduceDialog.Warning.Message.MongoReduceContainsUnresolvedVarsFieldSubs"));
      smd.open();
    }

    return !mapFunctionNotOk && !reduceFunctionNotOk;
  }

  private void setupDBNames() {
    final String current = wDbName.getText();
    wDbName.removeAll();

    final String hostname = transMeta.environmentSubstitute( wServers.getText());

    if (!StringUtils.isEmpty(hostname)) {
      final MongoDBMapReduceMeta meta = new MongoDBMapReduceMeta();
      getInfo(meta, false);
      try {
        final MongoClientWrapper wrapper = new MongoClientWrapper(meta, transMeta);
        List<String> dbNames = new ArrayList<String>();
        try {
          dbNames = wrapper.getDatabaseNames();
        } finally {
          wrapper.dispose();
        }

        for (String s : dbNames) {
          wDbName.add(s);
        }
      } catch (Exception e) {
        logError(
            BaseMessages.getString(PKG, "MongoDBMapReduceDialog.ErrorMessage.UnableToConnect"), e);
        new ErrorDialog(shell, BaseMessages.getString(PKG,
            "MongoDBMapReduceDialog.ErrorMessage.UnableToConnect"), BaseMessages.getString(PKG,
            "MongoDBMapReduceDialog.ErrorMessage.UnableToConnect"), e);
      }
    } else {
      // popup some feedback
      final MessageBox smd = new MessageBox(shell, SWT.ICON_WARNING | SWT.OK);
      smd.setText(BaseMessages.getString(PKG,
          "MongoDBMapReduceDialog.ErrorMessage.MissingConnectionDetails.Title"));
      smd.setMessage(BaseMessages.getString(PKG,
          "MongoDBMapReduceDialog.ErrorMessage.MissingConnectionDetails", "host name(s)"));
      smd.open();
    }

    if (!StringUtils.isEmpty(current)) {
      wDbName.setText(current);
    }
  }

  private void setupCollectionNamesForDB() {
    final String hostname = transMeta.environmentSubstitute( wServers.getText());
    final String dB = transMeta.environmentSubstitute(wDbName.getText());

    final String current = wCollection.getText();
    wCollection.removeAll();

    if (!StringUtils.isEmpty(hostname) && !StringUtils.isEmpty(dB)) {
      final MongoDBMapReduceMeta meta = new MongoDBMapReduceMeta();
      getInfo(meta, false);
      try {
        final MongoClientWrapper wrapper = new MongoClientWrapper(meta, transMeta);
        Set<String> collections = new HashSet<String>();
        try {
          collections = wrapper.getCollectionsNames(dB);
        } finally {
          wrapper.dispose();
        }

        for (String c : collections) {
          wCollection.add(c);
        }
      } catch (Exception e) {
        logError(
            BaseMessages.getString(PKG, "MongoDBMapReduceDialog.ErrorMessage.UnableToConnect"), e);
        new ErrorDialog(shell, BaseMessages.getString(PKG,
            "MongoDBMapReduceDialog.ErrorMessage.UnableToConnect"), BaseMessages.getString(PKG,
            "MongoDBMapReduceDialog.ErrorMessage.UnableToConnect"), e);
      }
    } else {
      // popup some feedback
      String missingConnDetails = "";
      if (StringUtils.isEmpty(hostname)) {
        missingConnDetails += "host name(s)";
      }
      if (StringUtils.isEmpty(dB)) {
        missingConnDetails += " database";
      }
      final MessageBox smd = new MessageBox(shell, SWT.ICON_WARNING | SWT.OK);
      smd.setText(BaseMessages.getString(PKG,
          "MongoDBMapReduceDialog.ErrorMessage.MissingConnectionDetails.Title"));
      smd.setMessage(BaseMessages.getString(PKG,
          "MongoDBMapReduceDialog.ErrorMessage.MissingConnectionDetails", missingConnDetails));
      smd.open();
    }

    if (!StringUtils.isEmpty(current)) {
      wCollection.setText(current);
    }
  }

  /**
   * Cancel.
   */
  private void cancel() {
    stepname = null;
    input.setChanged(backupChanged);
    dispose();
  }

  /**
   * Let the plugin know about the entered data.
   */
  private void ok() {
    if (!StringUtils.isEmpty(wStepname.getText())) {
      stepname = wStepname.getText();
      getInfo(input, true);
      dispose();
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData(MongoDBMapReduceMeta meta) {
    wServers.setText(Const.NVL(meta.getServers(), "localhost:27017"));
    wAuthUser.setText(Const.NVL(meta.getUsername(), ""));
    wAuthPass.setText(Const.NVL(meta.getPassword(), ""));
    wAuthDb.setText(Const.NVL(meta.getAuthDb(), ""));
    wAuthMechanism.setText(Const.NVL(meta.getAuthMechanism(), ""));

    wDbName.setText(Const.NVL(meta.getDatabaseName(), ""));
    wCollection.setText(Const.NVL(meta.getCollectionName(), ""));
    wJsonField.setText(Const.NVL(meta.getJsonField(), "json"));
    wMapFuncScript.setText(Const.NVL(meta.getMapFunction(), ""));
    wReduceFuncScript.setText(Const.NVL(meta.getReduceFunction(), ""));

    wOutputAsJson.setSelection(meta.isOutputAsJson());

    setFieldTableFields(meta.getFields());

    wJsonField.setEnabled(meta.isOutputAsJson());
    wGet.setEnabled(!meta.isOutputAsJson());
    wfieldsView.setEnabled(!meta.isOutputAsJson());

    wStepname.selectAll();
  }

  /**
   * Get the information.
   * 
   * @param info the push notification step meta data.
   */
  public void getInfo(MongoDBMapReduceMeta info, boolean validation) {
    info.setServers( Const.NVL(wServers.getText(), "localhost:27017"));
    info.setUsername(Const.NVL(wAuthUser.getText(), ""));
    info.setPassword(Const.NVL(wAuthPass.getText(), ""));
    info.setAuthDb(Const.NVL(wAuthDb.getText(), ""));
    info.setAuthMechanism(Const.NVL(wAuthMechanism.getText(), ""));

    if (validation && StringUtils.isEmpty(wDbName.getText())) {
      final MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG,
          "MongoDBMapReduceDialog.DbName.Mandatory.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
      mb.open();
      return;
    }
    info.setDatabaseName(Const.NVL(wDbName.getText(), ""));

    if (validation && StringUtils.isEmpty(wCollection.getText())) {
      final MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG,
          "MongoDBMapReduceDialog.Collection.Mandatory.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
      mb.open();
      return;
    }
    info.setCollectionName(Const.NVL(wCollection.getText(), ""));

    if (validation && StringUtils.isEmpty(wMapFuncScript.getText())) {
      final MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG,
          "MongoDBMapReduceDialog.MapFuncScript.Mandatory.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
      mb.open();
      return;
    }
    info.setMapFunction(Const.NVL(wMapFuncScript.getText(), ""));

    if (validation && StringUtils.isEmpty(wReduceFuncScript.getText())) {
      final MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG,
          "MongoDBMapReduceDialog.ReduceFuncScript.Mandatory.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
      mb.open();
      return;
    }
    info.setReduceFunction(Const.NVL(wReduceFuncScript.getText(), ""));
    info.setOutputAsJson(wOutputAsJson.getSelection());
    info.setJsonField(wJsonField.getText());

    final int numNonEmpty = wfieldsView.nrNonEmpty();
    if (numNonEmpty > 0) {
      final List<MongoField> outputFields = new ArrayList<MongoField>();
      for (int i = 0; i < numNonEmpty; i++) {
        final TableItem item = wfieldsView.getNonEmpty(i);
        final MongoField newField = new MongoField();
        newField.mFieldName = item.getText(1).trim();
        newField.mFieldPath = item.getText(2).trim();
        newField.mKettleType = item.getText(3).trim();
        outputFields.add(newField);
      }
      info.setFields(outputFields);
    } else if (validation && !wOutputAsJson.getSelection()) {
      final MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG,
          "MongoDBMapReduceDialog.Fields.Mandatory.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
      mb.open();
      return;
    }
  }

  private void setFieldTableFields(List<MongoField> fields) {
    if (fields == null) {
      return;
    }

    wfieldsView.clearAll();
    for (MongoField f : fields) {
      final TableItem item = new TableItem(wfieldsView.table, SWT.NONE);

      if (!StringUtils.isEmpty(f.mFieldName)) {
        item.setText(1, f.mFieldName);
      }

      if (!StringUtils.isEmpty(f.mFieldPath)) {
        item.setText(2, f.mFieldPath);
      }

      if (!StringUtils.isEmpty(f.mKettleType)) {
        item.setText(3, f.mKettleType);
      }
    }

    wfieldsView.removeEmptyRows();
    wfieldsView.setRowNums();
    wfieldsView.optWidth(true);
  }
}
