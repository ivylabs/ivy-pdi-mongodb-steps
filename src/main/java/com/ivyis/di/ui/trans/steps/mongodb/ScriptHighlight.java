package com.ivyis.di.ui.trans.steps.mongodb;

import java.io.IOException;
import java.io.StringReader;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.LineStyleEvent;
import org.eclipse.swt.custom.LineStyleListener;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.graphics.Color;
import org.pentaho.di.ui.core.gui.GUIResource;

/**
 * This class is responsible for highlight the key words of MongoDB map and reduce JavaScript
 * function.
 * 
 * @author <a href="mailto:joel.latino@ivy-is.co.uk">Joel Latino</a>
 * @since 1.0.0
 */
public class ScriptHighlight implements LineStyleListener {
  JavaScanner scanner = new JavaScanner();
  int[] tokenColors;
  Color[] colors;
  Vector<int[]> blockComments = new Vector<int[]>();

  public static final int EOF = -1;
  public static final int EOL = 10;

  public static final int WORD = 0;
  public static final int WHITE = 1;
  public static final int KEY = 2;
  public static final int COMMENT = 3; // single line comment: //
  public static final int STRING = 5;
  public static final int OTHER = 6;
  public static final int NUMBER = 7;
  public static final int FUNCTIONS = 8;

  public static final int MAXIMUM_TOKEN = 9;

  public ScriptHighlight() {
    initializeColors();
    scanner = new JavaScanner();
  }

  public ScriptHighlight(String[] strArrETLFunctions) {
    initializeColors();
    scanner = new JavaScanner();
    scanner.setETLKeywords(strArrETLFunctions);
    scanner.initializeETLFunctions();
  }

  Color getColor(int type) {
    if (type < 0 || type >= tokenColors.length) {
      return null;
    }
    return colors[tokenColors[type]];
  }

  boolean inBlockComment(int start, int end) {
    for (int i = 0; i < blockComments.size(); i++) {
      final int[] offsets = blockComments.elementAt(i);
      // start of comment in the line
      if ((offsets[0] >= start) && (offsets[0] <= end)) {
        return true;
      }
      // end of comment in the line
      if ((offsets[1] >= start) && (offsets[1] <= end)) {
        return true;
      }
      if ((offsets[0] <= start) && (offsets[1] >= end)) {
        return true;
      }
    }
    return false;
  }

  void initializeColors() {
    // Display display = Display.getDefault();
    colors = new Color[] {GUIResource.getInstance().getColor(0, 0, 0), // black
        GUIResource.getInstance().getColor(63, 127, 95), // red
        GUIResource.getInstance().getColor(0, 0, 192), // green
        GUIResource.getInstance().getColor(127, 0, 85), // blue
        GUIResource.getInstance().getColor(255, 102, 0) // Kettle Functions / Orange
    };
    tokenColors = new int[MAXIMUM_TOKEN];
    tokenColors[WORD] = 0;
    tokenColors[WHITE] = 0;
    tokenColors[KEY] = 3;
    tokenColors[COMMENT] = 1;
    tokenColors[STRING] = 2;
    tokenColors[OTHER] = 0;
    tokenColors[NUMBER] = 0;
    tokenColors[FUNCTIONS] = 4;
  }

  /**
   * Event.detail line start offset (input) Event.text line text (input) LineStyleEvent.styles
   * Enumeration of StyleRanges, need to be in order. (output) LineStyleEvent.background line
   * background color (output)
   */
  public void lineGetStyle(LineStyleEvent event) {
    final Vector<StyleRange> styles = new Vector<StyleRange>();
    int token;
    StyleRange lastStyle;

    if (inBlockComment(event.lineOffset, event.lineOffset + event.lineText.length())) {
      styles.addElement(new StyleRange(event.lineOffset, event.lineText.length() + 4, colors[2],
          null));
      event.styles = new StyleRange[styles.size()];
      styles.copyInto(event.styles);
      return;
    }
    scanner.setRange(event.lineText);
    final String xs = ((StyledText) event.widget).getText();
    if (xs != null) {
      parseBlockComments(xs);
    }
    token = scanner.nextToken();
    while (token != EOF) {
      if (token != OTHER) {
        if ((token == WHITE) && (!styles.isEmpty())) {
          final int start = scanner.getStartOffset() + event.lineOffset;
          lastStyle = styles.lastElement();
          if (lastStyle.fontStyle != SWT.NORMAL) {
            if (lastStyle.start + lastStyle.length == start) {
              // have the white space take on the style before it to minimize font style changes
              lastStyle.length += scanner.getLength();
            }
          }
        } else {
          final Color color = getColor(token);
          if (color != colors[0]) { // hardcoded default foreground color, black
            final StyleRange style =
                new StyleRange(scanner.getStartOffset() + event.lineOffset, scanner.getLength(),
                    color, null);
            if (token == KEY) {
              style.fontStyle = SWT.BOLD;
            }
            if (styles.isEmpty()) {
              styles.addElement(style);
            } else {
              lastStyle = styles.lastElement();
              if (lastStyle.similarTo(style) &&
                  (lastStyle.start + lastStyle.length == style.start)) {
                lastStyle.length += style.length;
              } else {
                styles.addElement(style);
              }
            }
          }
        }
      }
      token = scanner.nextToken();
    }
    event.styles = new StyleRange[styles.size()];
    styles.copyInto(event.styles);
  }

  public void parseBlockComments(String text) {
    blockComments = new Vector<int[]>();
    final StringReader buffer = new StringReader(text);
    int ch;
    boolean blkComment = false;
    int cnt = 0;
    int[] offsets = new int[2];
    boolean done = false;

    try {
      while (!done) {
        switch (ch = buffer.read()) {
          case -1: {
            if (blkComment) {
              offsets[1] = cnt;
              blockComments.addElement(offsets);
            }
            done = true;
            break;
          }
          case '/': {
            ch = buffer.read();
            if ((ch == '*') && (!blkComment)) {
              offsets = new int[2];
              offsets[0] = cnt;
              blkComment = true;
              cnt++;
            } else {
              cnt++;
            }
            cnt++;
            break;
          }
          case '*': {
            if (blkComment) {
              ch = buffer.read();
              cnt++;
              if (ch == '/') {
                blkComment = false;
                offsets[1] = cnt;
                blockComments.addElement(offsets);
              }
            }
            cnt++;
            break;
          }
          default: {
            cnt++;
            break;
          }
        }
      }
    } catch (IOException e) {
      // ignore errors
    }
  }

  /**
   * A simple fuzzy scanner for Java.
   */
  public class JavaScanner {

    protected Map<String, Integer> fgKeys = null;
    protected Map<?, ?> fgFunctions = null;
    protected Map<String, Integer> kfKeys = null;
    protected Map<?, ?> kfFunctions = null;
    protected StringBuffer fBuffer = new StringBuffer();
    protected String fDoc;
    protected int fPos;
    protected int fEnd;
    protected int fStartToken;
    protected boolean fEofSeen = false;

    private String[] kfKeywords = {"num2str"};

    private String[] fgKeywords = {"array", "break", "case", "catch", "const", "continue", "Date",
        "default", "delete", "do", "else", "eval", "escape", "false", "finally", "float", "for",
        "function", "if", "in", "instanceof", "isFinite", "isNaN", "new", "Number", "null",
        "String", "switch", "this", "then", "throw", "to", "true", "try", "typeof", "parseInt",
        "parseFloat", "return", "unescape", "var", "void", "with", "while"};

    public JavaScanner() {
      initialize();
      initializeETLFunctions();
    }

    /**
     * Returns the ending location of the current token in the document.
     */
    public final int getLength() {
      return fPos - fStartToken;
    }

    /**
     * Initialize the lookup table.
     */
    void initialize() {
      fgKeys = new Hashtable<String, Integer>();
      final Integer k = Integer.valueOf(KEY);
      for (int i = 0; i < fgKeywords.length; i++) {
        fgKeys.put(fgKeywords[i], k);
      }
    }

    public void setETLKeywords(String[] kfKeywords) {
      this.kfKeywords = kfKeywords;
    }

    void initializeETLFunctions() {
      kfKeys = new Hashtable<String, Integer>();
      final Integer k = Integer.valueOf(FUNCTIONS);
      for (int i = 0; i < kfKeywords.length; i++) {
        kfKeys.put(kfKeywords[i], k);
      }
    }

    /**
     * Returns the starting location of the current token in the document.
     */
    public final int getStartOffset() {
      return fStartToken;
    }

    /**
     * Returns the next lexical token in the document.
     */
    public int nextToken() {
      int c;
      fStartToken = fPos;
      while (true) {
        switch (c = read()) {
          case EOF:
            return EOF;
          case '/': // comment
            c = read();
            if (c == '/') {
              while (true) {
                c = read();
                if ((c == EOF) || (c == EOL)) {
                  unread(c);
                  return COMMENT;
                }
              }
            } else {
              unread(c);
            }
            return OTHER;
          case '\'': // char const
            c = read();
            switch (c) {
              case '\'':
                return STRING;
              case EOF:
                unread(c);
                return STRING;
              case '\\':
                c = read();
                break;
            }
            break;

          case '"': // string
            c = read();
            switch (c) {
              case '"':
                return STRING;
              case EOF:
                unread(c);
                return STRING;
              case '\\':
                c = read();
                break;
            }
            break;

          case '0':
          case '1':
          case '2':
          case '3':
          case '4':
          case '5':
          case '6':
          case '7':
          case '8':
          case '9':
            do {
              c = read();
            } while (Character.isDigit((char) c));
            unread(c);
            return NUMBER;
          default:
            if (Character.isWhitespace((char) c)) {
              do {
                c = read();
              } while (Character.isWhitespace((char) c));
              unread(c);
              return WHITE;
            }
            if (Character.isJavaIdentifierStart((char) c)) {
              fBuffer.setLength(0);
              do {
                fBuffer.append((char) c);
                c = read();
              } while (Character.isJavaIdentifierPart((char) c));
              unread(c);
              Integer i = fgKeys.get(fBuffer.toString());
              if (i != null) {
                return i.intValue();
              }
              i = kfKeys.get(fBuffer.toString());
              if (i != null) {
                return i.intValue();
              }
              return WORD;
            }
            return OTHER;
        }
      }
    }

    /**
     * Returns next character.
     */
    protected int read() {
      if (fPos <= fEnd) {
        return fDoc.charAt(fPos++);
      }
      return EOF;
    }

    public void setRange(String text) {
      fDoc = text;
      fPos = 0;
      fEnd = fDoc.length() - 1;
    }

    protected void unread(int c) {
      if (c != EOF) {
        fPos--;
      }
    }
  }
}
