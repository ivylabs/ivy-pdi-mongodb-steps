package com.ivyis.di.ui.trans.steps.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
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
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.ShowBrowserDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.LabelTextVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;
import org.pentaho.di.ui.util.ImageUtil;

import com.ivyis.di.trans.steps.mongodb.MongoClientWrapper;
import com.ivyis.di.trans.steps.mongodb.MongoDBLookupData;
import com.ivyis.di.trans.steps.mongodb.MongoDBLookupMeta;

/**
 * This class is responsible for the UI in Spoon of MongoDB lookup step.
 * 
 * @author <a href="mailto:joel.latino@ivy-is.co.uk">Joel Latino</a>
 * @since 1.0.0
 */
public class MongoDBLookupDialog extends BaseStepDialog implements
    StepDialogInterface {

  /** for i18n purposes. **/
  private static final Class<?> PKG = MongoDBLookupDialog.class;

  private CTabFolder wTabFolder;
  private CTabItem wMongoConfigTab;
  private CTabItem wMongoFieldsTab;

  private TextVar wHostname;
  private TextVar wPort;
  private TextVar wAuthUser;
  private LabelTextVar wAuthPass;
  private CCombo wDbName;
  private Button wgetDbsBut;
  private CCombo wCollection;
  private Button wgetCollectionsBut;

  private TableView wKey;
  private TableView wReturn;
  // private Text wOrderBy
  private Button wFailMultiple;
  private Button wEatRows;
  private Button wGet, wGetLU;
  private Listener lsGet, lsGetLU;
  private ColumnInfo[] ciKey;

  private MongoDBLookupMeta input;
  private Shell parent;

  private Map<String, Integer> inputFields;

  public MongoDBLookupDialog(Shell parent, BaseStepMeta in, TransMeta transMeta, String sname) {
    super(parent, in, transMeta, sname);
    this.input = (MongoDBLookupMeta) in;
    inputFields = new HashMap<String, Integer>();
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
    shell.setText(BaseMessages.getString(PKG, "MongoDBLookupDialog.Shell.Title"));

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

    final ScrolledComposite sc = new ScrolledComposite(shell, SWT.H_SCROLL | SWT.V_SCROLL);

    wTabFolder = new CTabFolder(sc, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // MongoDB config TAB
    wMongoConfigTab = new CTabItem(wTabFolder, SWT.NONE);
    wMongoConfigTab.setText(BaseMessages.getString(PKG,
        "MongoDBLookupDialog.MongoConfigTab.CTabItem.Title"));

    final FormLayout mainOptionsLayout = new FormLayout();
    mainOptionsLayout.marginWidth = Const.MARGIN;
    mainOptionsLayout.marginHeight = Const.MARGIN;

    final Composite wMongoConfigComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wMongoConfigComp);
    wMongoConfigComp.setLayout(mainOptionsLayout);

    // Hostname line...
    final Label wlHostname = new Label(wMongoConfigComp, SWT.RIGHT);
    wlHostname.setText(BaseMessages.getString(PKG, "MongoDBLookupDialog.Hostname.Label"));
    props.setLook(wlHostname);
    final FormData fdlHostname = new FormData();
    fdlHostname.left = new FormAttachment(0, 0);
    fdlHostname.right = new FormAttachment(middle, -margin);
    fdlHostname.top = new FormAttachment(0, margin);
    wlHostname.setLayoutData(fdlHostname);

    wHostname = new TextVar(transMeta, wMongoConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wHostname);
    wHostname.addModifyListener(lsMod);
    final FormData fdHostname = new FormData();
    fdHostname.left = new FormAttachment(middle, 0);
    fdHostname.top = new FormAttachment(0, margin);
    fdHostname.right = new FormAttachment(100, 0);
    wHostname.setLayoutData(fdHostname);

    // Port line...
    final Label wlPort = new Label(wMongoConfigComp, SWT.RIGHT);
    wlPort.setText(BaseMessages.getString(PKG, "MongoDBLookupDialog.Port.Label"));
    props.setLook(wlPort);
    final FormData fdlPort = new FormData();
    fdlPort.left = new FormAttachment(0, 0);
    fdlPort.right = new FormAttachment(middle, -margin);
    fdlPort.top = new FormAttachment(wHostname, margin * 2);
    wlPort.setLayoutData(fdlPort);

    wPort = new TextVar(transMeta, wMongoConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPort);
    wPort.addModifyListener(lsMod);
    final FormData fdPort = new FormData();
    fdPort.left = new FormAttachment(middle, 0);
    fdPort.top = new FormAttachment(wHostname, margin * 2);
    fdPort.right = new FormAttachment(100, 0);
    wPort.setLayoutData(fdPort);

    // UserName field
    final Label wlAuthUser = new Label(wMongoConfigComp, SWT.RIGHT);
    wlAuthUser.setText(BaseMessages.getString(PKG, "MongoDBLookupDialog.AuthUser.Label"));
    props.setLook(wlAuthUser);
    final FormData fdlAuthUser = new FormData();
    fdlAuthUser.left = new FormAttachment(0, 0);
    fdlAuthUser.top = new FormAttachment(wPort, margin * 2);
    fdlAuthUser.right = new FormAttachment(middle, -margin);
    wlAuthUser.setLayoutData(fdlAuthUser);

    wAuthUser = new TextVar(this.transMeta, wMongoConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wAuthUser.setEditable(true);
    props.setLook(wAuthUser);
    final FormData fdAuthUser = new FormData();
    fdAuthUser.left = new FormAttachment(middle, 0);
    fdAuthUser.top = new FormAttachment(wPort, margin * 2);
    fdAuthUser.right = new FormAttachment(100, 0);
    wAuthUser.setLayoutData(fdAuthUser);

    // Password field
    wAuthPass =
        new LabelTextVar(transMeta, wMongoConfigComp, BaseMessages.getString(PKG,
            "MongoDBLookupDialog.AuthPass.Label"), BaseMessages.getString(PKG,
            "MongoDBLookupDialog.AuthPass.Tooltip"));
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

    // DbName input ...
    //
    final Label wlDbName = new Label(wMongoConfigComp, SWT.RIGHT);
    wlDbName.setText(BaseMessages.getString(PKG, "MongoDBLookupDialog.DbName.Label"));
    props.setLook(wlDbName);
    final FormData fdlDbName = new FormData();
    fdlDbName.left = new FormAttachment(0, 0);
    fdlDbName.right = new FormAttachment(middle, -margin);
    fdlDbName.top = new FormAttachment(wAuthPass, margin);
    wlDbName.setLayoutData(fdlDbName);

    wgetDbsBut = new Button(wMongoConfigComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wgetDbsBut);
    wgetDbsBut.setText(BaseMessages.getString(PKG, "MongoDBLookupDialog.DbName.Button"));
    final FormData fd = new FormData();
    fd.top = new FormAttachment(wAuthPass, margin);
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
    fdDbName.top = new FormAttachment(wAuthPass, margin);
    fdDbName.right = new FormAttachment(wgetDbsBut, 0);
    wDbName.setLayoutData(fdDbName);

    wDbName.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        input.setChanged();
        wDbName.setToolTipText(transMeta.environmentSubstitute(wDbName.getText()));
      }
    });

    // Collection input ...
    final Label wlCollection = new Label(wMongoConfigComp, SWT.RIGHT);
    wlCollection.setText(BaseMessages.getString(PKG, "MongoDBLookupDialog.Collection.Label"));
    props.setLook(wlCollection);
    final FormData fdlCollection = new FormData();
    fdlCollection.left = new FormAttachment(0, 0);
    fdlCollection.right = new FormAttachment(middle, -margin);
    fdlCollection.top = new FormAttachment(wDbName, margin);
    wlCollection.setLayoutData(fdlCollection);

    wgetCollectionsBut = new Button(wMongoConfigComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wgetCollectionsBut);
    wgetCollectionsBut.setText(BaseMessages.getString(PKG,
        "MongoDBLookupDialog.GetCollections.Button"));
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

    // fields tab -----
    wMongoFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wMongoFieldsTab.setText(BaseMessages.getString(PKG, "MongoDBLookupDialog.FieldsTab.TabTitle"));
    final Composite wMongoFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wMongoFieldsComp);
    final FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;
    wMongoFieldsComp.setLayout(fieldsLayout);

    final Label wlKey = new Label(wMongoFieldsComp, SWT.NONE);
    wlKey.setText(BaseMessages.getString(PKG, "MongoDBLookupDialog.Keys.Label"));
    props.setLook(wlKey);
    final FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment(0, 0);
    fdlKey.right = new FormAttachment(middle, -margin);
    fdlKey.top = new FormAttachment(0, margin);
    wlKey.setLayoutData(fdlKey);

    final int nrKeyCols = 3;
    final int nrKeyRows =
        (input.getStreamKeyField1() != null ? input.getStreamKeyField1().length : 1);

    ciKey = new ColumnInfo[nrKeyCols];
    ciKey[0] =
        new ColumnInfo(BaseMessages.getString(PKG, "MongoDBLookupDialog.ColumnInfo.FieldPath"),
            ColumnInfo.COLUMN_TYPE_TEXT, new String[] {}, false);
    ciKey[1] =
        new ColumnInfo(BaseMessages.getString(PKG, "MongoDBLookupDialog.ColumnInfo.Comparator"),
            ColumnInfo.COLUMN_TYPE_CCOMBO, MongoDBLookupMeta.CONDITION_STRINGS);
    ciKey[2] =
        new ColumnInfo(BaseMessages.getString(PKG, "MongoDBLookupDialog.ColumnInfo.Field1"),
            ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {}, false);
    wKey =
        new TableView(transMeta, wMongoFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI
            | SWT.V_SCROLL | SWT.H_SCROLL, ciKey, nrKeyRows, lsMod, props);

    final FormData fdKey = new FormData();
    fdKey.left = new FormAttachment(0, 0);
    fdKey.top = new FormAttachment(wlKey, margin);
    fdKey.right = new FormAttachment(100, 0);
    fdKey.bottom = new FormAttachment(wlKey, 150);
    wKey.setLayoutData(fdKey);


    // Search the fields in the background
    final Runnable runnable = new Runnable() {
      public void run() {
        final StepMeta stepMeta = transMeta.findStep(stepname);
        if (stepMeta != null) {
          try {
            final RowMetaInterface row = transMeta.getPrevStepFields(stepMeta);

            // Remember these fields...
            for (int i = 0; i < row.size(); i++) {
              inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
            }

            setComboBoxes();
          } catch (KettleException e) {
            logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
          }
        }
      }
    };
    new Thread(runnable).start();


    final Label wlReturn = new Label(wMongoFieldsComp, SWT.NONE);
    wlReturn.setText(BaseMessages.getString(PKG, "MongoDBLookupDialog.Return.Label"));
    props.setLook(wlReturn);
    final FormData fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment(0, 0);
    fdlReturn.top = new FormAttachment(wKey, margin);
    wlReturn.setLayoutData(fdlReturn);

    final int upInsCols = 4;
    final int upInsRows =
        (input.getReturnValueField() != null ? input.getReturnValueField().length : 1);

    final ColumnInfo[] ciReturn = new ColumnInfo[upInsCols];
    ciReturn[0] =
        new ColumnInfo(BaseMessages.getString(PKG, "MongoDBLookupDialog.ColumnInfo.Field"),
            ColumnInfo.COLUMN_TYPE_TEXT, new String[] {}, false);
    ciReturn[1] =
        new ColumnInfo(BaseMessages.getString(PKG, "MongoDBLookupDialog.ColumnInfo.Path"),
            ColumnInfo.COLUMN_TYPE_TEXT, false);
    ciReturn[2] =
        new ColumnInfo(BaseMessages.getString(PKG, "MongoDBLookupDialog.ColumnInfo.Default"),
            ColumnInfo.COLUMN_TYPE_TEXT, false);
    ciReturn[3] =
        new ColumnInfo(BaseMessages.getString(PKG, "MongoDBLookupDialog.ColumnInfo.Type"),
            ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMeta.getTypes());

    wReturn =
        new TableView(transMeta, wMongoFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI
            | SWT.V_SCROLL | SWT.H_SCROLL, ciReturn, upInsRows, lsMod, props);

    final FormData fdReturn = new FormData();
    fdReturn.left = new FormAttachment(0, 0);
    fdReturn.top = new FormAttachment(wlReturn, margin);
    fdReturn.right = new FormAttachment(100, 0);
    fdReturn.bottom = new FormAttachment(wlReturn, 250);
    wReturn.setLayoutData(fdReturn);

    // EatRows?
    final Label wlEatRows = new Label(wMongoFieldsComp, SWT.RIGHT);
    wlEatRows.setText(BaseMessages.getString(PKG, "MongoDBLookupDialog.EatRows.Label"));
    props.setLook(wlEatRows);
    final FormData fdlEatRows = new FormData();
    fdlEatRows.left = new FormAttachment(0, 0);
    fdlEatRows.right = new FormAttachment(middle, -margin);
    fdlEatRows.top = new FormAttachment(wReturn, margin);
    wlEatRows.setLayoutData(fdlEatRows);
    wEatRows = new Button(wMongoFieldsComp, SWT.CHECK);
    props.setLook(wEatRows);
    final FormData fdEatRows = new FormData();
    fdEatRows.left = new FormAttachment(middle, 0);
    fdEatRows.right = new FormAttachment(100, 0);
    fdEatRows.top = new FormAttachment(wReturn, margin);
    wEatRows.setLayoutData(fdEatRows);
    wEatRows.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected(SelectionEvent e) {
        input.setChanged();
        enableFields();
      }
    });

    // FailMultiple?
    final Label wlFailMultiple = new Label(wMongoFieldsComp, SWT.RIGHT);
    wlFailMultiple.setText(BaseMessages.getString(PKG, "MongoDBLookupDialog.FailMultiple.Label"));
    props.setLook(wlFailMultiple);
    final FormData fdlFailMultiple = new FormData();
    fdlFailMultiple.left = new FormAttachment(0, 0);
    fdlFailMultiple.right = new FormAttachment(middle, -margin);
    fdlFailMultiple.top = new FormAttachment(wEatRows, margin);
    wlFailMultiple.setLayoutData(fdlFailMultiple);
    wFailMultiple = new Button(wMongoFieldsComp, SWT.CHECK);
    props.setLook(wFailMultiple);
    final FormData fdFailMultiple = new FormData();
    fdFailMultiple.left = new FormAttachment(middle, 0);
    fdFailMultiple.right = new FormAttachment(100, 0);
    fdFailMultiple.top = new FormAttachment(wEatRows, margin);
    wFailMultiple.setLayoutData(fdFailMultiple);
    wFailMultiple.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected(SelectionEvent e) {
        input.setChanged();
        enableFields();
      }
    });

    // OderBy line
    // final Label wlOrderBy = new Label(wMongoFieldsComp, SWT.RIGHT);
    // wlOrderBy.setText(BaseMessages.getString(PKG,
    // "DatabaseLookupDialog.Orderby.Label"));
    // props.setLook(wlOrderBy);
    // final FormData fdlOrderBy = new FormData();
    // fdlOrderBy.left = new FormAttachment(0, 0);
    // fdlOrderBy.right = new FormAttachment(middle, -margin);
    // fdlOrderBy.top = new FormAttachment(wFailMultiple, margin);
    // wlOrderBy.setLayoutData(fdlOrderBy);
    // wOrderBy = new Text(wMongoFieldsComp, SWT.SINGLE | SWT.LEFT
    // | SWT.BORDER);
    // props.setLook(wOrderBy);
    // final FormData fdOrderBy = new FormData();
    // fdOrderBy.left = new FormAttachment(middle, 0);
    // fdOrderBy.right = new FormAttachment(100, 0);
    // fdOrderBy.top = new FormAttachment(wFailMultiple, margin);
    // wOrderBy.setLayoutData(fdOrderBy);
    // wOrderBy.addModifyListener(lsMod);

    final FormData fdMongoFieldsComp = new FormData();
    fdMongoFieldsComp.left = new FormAttachment(0, 0);
    fdMongoFieldsComp.top = new FormAttachment(0, 0);
    fdMongoFieldsComp.right = new FormAttachment(100, 0);
    fdMongoFieldsComp.bottom = new FormAttachment(100, 0);
    wMongoFieldsComp.setLayoutData(fdMongoFieldsComp);

    wMongoFieldsComp.layout();
    wMongoFieldsTab.setControl(wMongoFieldsComp);

    // THE BUTTONS
    wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "MongoDBLookupDialog.GetFields.Button"));
    wGetLU = new Button(shell, SWT.PUSH);
    wGetLU.setText(BaseMessages.getString(PKG, "MongoDBLookupDialog.GetLookupFields.Button"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOK, wCancel, wGet, wGetLU}, margin, sc);

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
    sc.setLayoutData(fdSc);

    sc.setContent(wTabFolder);

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent(Event e) {
        ok();
      }
    };
    lsGet = new Listener() {
      public void handleEvent(Event e) {
        get();
      }
    };
    lsGetLU = new Listener() {
      public void handleEvent(Event e) {
        getlookup();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent(Event e) {
        cancel();
      }
    };

    wOK.addListener(SWT.Selection, lsOK);
    wGet.addListener(SWT.Selection, lsGet);
    wGetLU.addListener(SWT.Selection, lsGetLU);
    wCancel.addListener(SWT.Selection, lsCancel);

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };

    wStepname.addSelectionListener(lsDef);
    // wOrderBy.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(new ShellAdapter() {
      public void shellClosed(ShellEvent e) {
        cancel();
      }
    });

    // Set the shell size, based upon previous time...
    setSize();

    getData(input);

    // determine scrollable area
    sc.setMinSize(wTabFolder.computeSize(SWT.DEFAULT, SWT.DEFAULT));
    sc.setExpandHorizontal(true);
    sc.setExpandVertical(true);

    // set window size
    BaseStepDialog.setSize(shell, 600, 400, true);

    input.setChanged(backupChanged);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return stepname;
  }

  protected void getlookup() {
    if (!Const.isEmpty(wHostname.getText()) && !Const.isEmpty(wDbName.getText())
        && !Const.isEmpty(wCollection.getText())) {
      final int samples = 100;
      if (samples > 0) {
        try {
          getInfo(input, false);

          final boolean result = MongoDBLookupData.discoverFields(input, transMeta, samples);

          if (!result) {
            new ErrorDialog(shell, stepname, BaseMessages.getString(PKG,
                "MongoDBLookupDialog.ErrorMessage.NoFieldsFound"), new KettleException(
                BaseMessages.getString(PKG, "MongoDBLookupDialog.ErrorMessage.NoFieldsFound")));
          } else {
            getData(input);
          }
        } catch (KettleException e) {
          new ErrorDialog(shell, stepname, BaseMessages.getString(PKG,
              "MongoDBLookupDialog.ErrorMessage.ErrorDuringSampling"), e);
        } catch (UnknownHostException e) {
          new ErrorDialog(shell, stepname, BaseMessages.getString(PKG,
              "MongoDBLookupDialog.ErrorMessage.UnknownHost"), e);
        }
      }
    } else {
      // pop up an error dialog
      String missingConDetails = "";
      if (Const.isEmpty(wHostname.getText())) {
        missingConDetails += " host name(s)";
      }
      if (Const.isEmpty(wDbName.getText())) {
        missingConDetails += " database";
      }
      if (Const.isEmpty(wCollection.getText())) {
        missingConDetails += " collection";
      }
      new ErrorDialog(shell, stepname, BaseMessages.getString(PKG,
          "MongoDBLookupDialog.ErrorMessage.MissingConnectionDetails.Title"),
          new KettleException(BaseMessages.getString(PKG,
              "MongoDBLookupDialog.ErrorMessage.NoFieldsFound", missingConDetails)));
    }

  }

  private void get() {
    try {
      final RowMetaInterface r = transMeta.getPrevStepFields(stepname);
      if (r != null && !r.isEmpty()) {
        final TableItemInsertListener listener = new TableItemInsertListener() {
          public boolean tableItemInserted(TableItem tableItem, ValueMetaInterface v) {
            tableItem.setText(2, "=");
            return true;
          }
        };
        BaseStepDialog.getFieldsFromPrevious(r, wKey, 1, new int[] {1, 3}, new int[] {}, -1, -1,
            listener);
      }
    } catch (KettleException ke) {
      new ErrorDialog(shell, BaseMessages.getString(PKG,
          "MongoDBLookupDialog.GetFieldsFailed.DialogTitle"), BaseMessages.getString(PKG,
          "MongoDBLookupDialog.GetFieldsFailed.DialogMessage"), ke);
    }
  }

  private void enableFields() {
    // wOrderBy.setEnabled(!wFailMultiple.getSelection());
  }

  private void setupDBNames() {
    final String current = wDbName.getText();
    wDbName.removeAll();

    final String hostname = transMeta.environmentSubstitute(wHostname.getText());

    if (!Const.isEmpty(hostname)) {
      final MongoDBLookupMeta meta = new MongoDBLookupMeta();
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
        logError(BaseMessages.getString(PKG, "MongoDBLookupDialog.ErrorMessage.UnableToConnect"),
            e);
        new ErrorDialog(shell, BaseMessages.getString(PKG,
            "MongoDBLookupDialog.ErrorMessage.UnableToConnect"), BaseMessages.getString(PKG,
            "MongoDBLookupDialog.ErrorMessage.UnableToConnect"), e);
      }
    } else {
      // popup some feedback
      final MessageBox smd = new MessageBox(shell, SWT.ICON_WARNING | SWT.OK);
      smd.setText(BaseMessages.getString(PKG,
          "MongoDBLookupDialog.ErrorMessage.MissingConnectionDetails.Title"));
      smd.setMessage(BaseMessages.getString(PKG,
          "MongoDBLookupDialog.ErrorMessage.MissingConnectionDetails", "host name(s)"));
      smd.open();
    }

    if (!Const.isEmpty(current)) {
      wDbName.setText(current);
    }
  }

  private void setupCollectionNamesForDB() {
    final String hostname = transMeta.environmentSubstitute(wHostname.getText());
    final String dB = transMeta.environmentSubstitute(wDbName.getText());

    final String current = wCollection.getText();
    wCollection.removeAll();

    if (!Const.isEmpty(hostname) && !Const.isEmpty(dB)) {
      final MongoDBLookupMeta meta = new MongoDBLookupMeta();
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
        logError(BaseMessages.getString(PKG, "MongoDBLookupDialog.ErrorMessage.UnableToConnect"),
            e);
        new ErrorDialog(shell, BaseMessages.getString(PKG,
            "MongoDBLookupDialog.ErrorMessage.UnableToConnect"), BaseMessages.getString(PKG,
            "MongoDBLookupDialog.ErrorMessage.UnableToConnect"), e);
      }
    } else {
      // popup some feedback

      String missingConnDetails = "";
      if (Const.isEmpty(hostname)) {
        missingConnDetails += "host name(s)";
      }
      if (Const.isEmpty(dB)) {
        missingConnDetails += " database";
      }
      final MessageBox smd = new MessageBox(shell, SWT.ICON_WARNING | SWT.OK);
      smd.setText(BaseMessages.getString(PKG,
          "MongoDBLookupDialog.ErrorMessage.MissingConnectionDetails.Title"));
      smd.setMessage(BaseMessages.getString(PKG,
          "MongoDBLookupDialog.ErrorMessage.MissingConnectionDetails", missingConnDetails));
      smd.open();
    }

    if (!Const.isEmpty(current)) {
      wCollection.setText(current);
    }
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
    if (!Const.isEmpty(wStepname.getText())) {
      stepname = wStepname.getText();
      getInfo(input, true);
      dispose();
    }
  }

  /**
   * Set the steam fields on the ComboBoxes.
   */
  protected void setComboBoxes() {
    // Something was changed in the row.
    final Map<String, Integer> fields = new HashMap<String, Integer>();

    // Add the currentMeta fields...
    fields.putAll(inputFields);

    final Set<String> keySet = fields.keySet();
    final List<String> entries = new ArrayList<String>(keySet);

    final String[] fieldNames = entries.toArray(new String[entries.size()]);

    Const.sortStrings(fieldNames);
    ciKey[2].setComboValues(fieldNames);
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData(MongoDBLookupMeta meta) {
    wHostname.setText(Const.NVL(meta.getHostname(), "localhost"));
    wPort.setText(Const.NVL(meta.getPort(), MongoClientWrapper.MONGODB_DEFAUL_PORT));
    wDbName.setText(Const.NVL(meta.getDatabaseName(), ""));
    wCollection.setText(Const.NVL(meta.getCollectionName(), ""));
    wAuthUser.setText(Const.NVL(meta.getUsername(), ""));
    wAuthPass.setText(Const.NVL(meta.getPassword(), ""));

    if (meta.getStreamKeyField1() != null) {
      wKey.clearAll();
      for (int i = 0; i < meta.getStreamKeyField1().length; i++) {
        final TableItem item = new TableItem(wKey.table, SWT.NONE);
        if (meta.getCollectionKeyField()[i] != null) {
          item.setText(1, meta.getCollectionKeyField()[i]);
        }
        if (meta.getKeyCondition()[i] != null) {
          item.setText(2, meta.getKeyCondition()[i]);
        }
        if (meta.getStreamKeyField1()[i] != null) {
          item.setText(3, meta.getStreamKeyField1()[i]);
        }
      }
    }

    if (meta.getReturnValueField() != null) {
      wReturn.clearAll();
      for (int i = 0; i < meta.getReturnValueField().length; i++) {
        final TableItem item = new TableItem(wReturn.table, SWT.NONE);

        if (meta.getReturnValueNewName()[i] != null
            && !meta.getReturnValueNewName()[i].equals(meta.getReturnValueField()[i])) {
          item.setText(1, meta.getReturnValueNewName()[i]);
        }

        if (meta.getReturnValueField()[i] != null) {
          item.setText(2, meta.getReturnValueField()[i]);
        }

        if (meta.getReturnValueDefault()[i] != null) {
          item.setText(3, meta.getReturnValueDefault()[i]);
        }
        item.setText(4, ValueMeta.getTypeDesc(meta.getReturnValueDefaultType()[i]));
      }
    }

    wFailMultiple.setSelection(meta.isFailingOnMultipleResults());
    wEatRows.setSelection(meta.isEatingRowOnLookupFailure());

    wKey.removeEmptyRows();
    wKey.setRowNums();
    wKey.optWidth(true);
    wReturn.removeEmptyRows();
    wReturn.setRowNums();
    wReturn.optWidth(true);

    enableFields();

    wStepname.selectAll();
  }

  /**
   * Get the information.
   * 
   * @param info the push notification step meta data.
   */
  public void getInfo(MongoDBLookupMeta info, boolean validation) {
    if (Const.isEmpty(wHostname.getText())) {
      wHostname.setText("localhost");
    }
    info.setHostname(wHostname.getText());

    if (Const.isEmpty(wPort.getText())) {
      wPort.setText(MongoClientWrapper.MONGODB_DEFAUL_PORT);
    }
    info.setPort(wPort.getText());

    info.setUsername(Const.NVL(wAuthUser.getText(), ""));
    info.setPassword(Const.NVL(wAuthPass.getText(), ""));

    if (validation && Const.isEmpty(wDbName.getText())) {
      final MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG,
          "MongoDBLookupDialog.DbName.Mandatory.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
      mb.open();
      return;
    }
    info.setDatabaseName(Const.NVL(wDbName.getText(), ""));

    if (validation && Const.isEmpty(wCollection.getText())) {
      final MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG,
          "MongoDBLookupDialog.Collection.Mandatory.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
      mb.open();
      return;
    }
    info.setCollectionName(Const.NVL(wCollection.getText(), ""));

    if (validation && wKey.nrNonEmpty() == 0) {
      final MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG,
          "MongoDBLookupDialog.keys.Mandatory.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
      mb.open();
      return;
    }

    int numNonEmpty = wKey.nrNonEmpty();
    if (numNonEmpty > 0) {
      final String[] keyFields = new String[numNonEmpty];
      final String[] keyConditions = new String[numNonEmpty];
      final String[] collectionKeys = new String[numNonEmpty];
      for (int i = 0; i < numNonEmpty; i++) {
        final TableItem item = wKey.getNonEmpty(i);
        collectionKeys[i] = item.getText(1).trim();
        keyConditions[i] = item.getText(2).trim();
        keyFields[i] = item.getText(3).trim();
      }
      info.setKeyCondition(keyConditions);
      info.setStreamKeyField1(keyFields);
      info.setCollectionKeyField(collectionKeys);
    } else if (validation) {
      final MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG,
          "MongoDBLookupDialog.Keys.Mandatory.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
      mb.open();
      return;
    }

    numNonEmpty = wReturn.nrNonEmpty();
    if (numNonEmpty > 0) {
      final String[] returnValueFields = new String[numNonEmpty];
      final String[] returnValueNewNames = new String[numNonEmpty];
      final String[] returnValueDefaults = new String[numNonEmpty];
      final int[] returnValueDefaultTypes = new int[numNonEmpty];
      for (int i = 0; i < numNonEmpty; i++) {
        final TableItem item = wReturn.getNonEmpty(i);
        returnValueNewNames[i] = item.getText(1).trim();
        returnValueFields[i] = item.getText(2).trim();
        returnValueDefaults[i] = item.getText(3).trim();
        returnValueDefaultTypes[i] = ValueMeta.getType(item.getText(4));
        if (returnValueDefaultTypes[i] < 0) {
          returnValueDefaultTypes[i] = ValueMetaInterface.TYPE_STRING;
        }
      }
      info.setReturnValueDefault(returnValueDefaults);
      info.setReturnValueDefaultType(returnValueDefaultTypes);
      info.setReturnValueNewName(returnValueNewNames);
      info.setReturnValueField(returnValueFields);
    } else if (validation) {
      final MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG,
          "MongoDBLookupDialog.ReturnNewFields.Mandatory.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
      mb.open();
      return;
    }

    info.setFailingOnMultipleResults(wFailMultiple.getSelection());
    info.setEatingRowOnLookupFailure(wEatRows.getSelection());
    wStepname.selectAll();
  }
}
