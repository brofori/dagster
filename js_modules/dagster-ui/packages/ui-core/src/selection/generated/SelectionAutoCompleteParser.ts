// Generated from /Users/marcosalazar/code/dagster/js_modules/dagster-ui/packages/ui-core/src/selection/SelectionAutoComplete.g4 by ANTLR 4.9.0-SNAPSHOT

import {FailedPredicateException} from 'antlr4ts/FailedPredicateException';
import {NoViableAltException} from 'antlr4ts/NoViableAltException';
import {Parser} from 'antlr4ts/Parser';
import {ParserRuleContext} from 'antlr4ts/ParserRuleContext';
import {RecognitionException} from 'antlr4ts/RecognitionException';
import {RuleContext} from 'antlr4ts/RuleContext';
//import { RuleVersion } from "antlr4ts/RuleVersion";
import {TokenStream} from 'antlr4ts/TokenStream';
import {Vocabulary} from 'antlr4ts/Vocabulary';
import {VocabularyImpl} from 'antlr4ts/VocabularyImpl';
import {ATN} from 'antlr4ts/atn/ATN';
import {ATNDeserializer} from 'antlr4ts/atn/ATNDeserializer';
import {ParserATNSimulator} from 'antlr4ts/atn/ParserATNSimulator';
import * as Utils from 'antlr4ts/misc/Utils';
import {TerminalNode} from 'antlr4ts/tree/TerminalNode';

import {SelectionAutoCompleteListener} from './SelectionAutoCompleteListener';
import {SelectionAutoCompleteVisitor} from './SelectionAutoCompleteVisitor';

export class SelectionAutoCompleteParser extends Parser {
  public static readonly AND = 1;
  public static readonly OR = 2;
  public static readonly NOT = 3;
  public static readonly STAR = 4;
  public static readonly PLUS = 5;
  public static readonly COLON = 6;
  public static readonly LPAREN = 7;
  public static readonly RPAREN = 8;
  public static readonly EQUAL = 9;
  public static readonly QUOTED_STRING = 10;
  public static readonly INCOMPLETE_LEFT_QUOTED_STRING = 11;
  public static readonly INCOMPLETE_RIGHT_QUOTED_STRING = 12;
  public static readonly IDENTIFIER = 13;
  public static readonly WS = 14;
  public static readonly RULE_start = 0;
  public static readonly RULE_expr = 1;
  public static readonly RULE_traversalAllowedExpr = 2;
  public static readonly RULE_parenthesizedExpr = 3;
  public static readonly RULE_incompleteExpr = 4;
  public static readonly RULE_expressionLessParenthesizedExpr = 5;
  public static readonly RULE_upTraversalExpr = 6;
  public static readonly RULE_downTraversalExpr = 7;
  public static readonly RULE_traversal = 8;
  public static readonly RULE_attributeName = 9;
  public static readonly RULE_attributeValue = 10;
  public static readonly RULE_functionName = 11;
  public static readonly RULE_orToken = 12;
  public static readonly RULE_andToken = 13;
  public static readonly RULE_notToken = 14;
  public static readonly RULE_colonToken = 15;
  public static readonly RULE_leftParenToken = 16;
  public static readonly RULE_rightParenToken = 17;
  public static readonly RULE_attributeValueWhitespace = 18;
  public static readonly RULE_postAttributeValueWhitespace = 19;
  public static readonly RULE_postExpressionWhitespace = 20;
  public static readonly RULE_postNotOperatorWhitespace = 21;
  public static readonly RULE_postLogicalOperatorWhitespace = 22;
  public static readonly RULE_postNeighborTraversalWhitespace = 23;
  public static readonly RULE_postUpwardTraversalWhitespace = 24;
  public static readonly RULE_postDownwardTraversalWhitespace = 25;
  public static readonly RULE_value = 26;
  // tslint:disable:no-trailing-whitespace
  public static readonly ruleNames: string[] = [
    'start',
    'expr',
    'traversalAllowedExpr',
    'parenthesizedExpr',
    'incompleteExpr',
    'expressionLessParenthesizedExpr',
    'upTraversalExpr',
    'downTraversalExpr',
    'traversal',
    'attributeName',
    'attributeValue',
    'functionName',
    'orToken',
    'andToken',
    'notToken',
    'colonToken',
    'leftParenToken',
    'rightParenToken',
    'attributeValueWhitespace',
    'postAttributeValueWhitespace',
    'postExpressionWhitespace',
    'postNotOperatorWhitespace',
    'postLogicalOperatorWhitespace',
    'postNeighborTraversalWhitespace',
    'postUpwardTraversalWhitespace',
    'postDownwardTraversalWhitespace',
    'value',
  ];

  private static readonly _LITERAL_NAMES: Array<string | undefined> = [
    undefined,
    "'and'",
    "'or'",
    "'not'",
    "'*'",
    "'+'",
    "':'",
    "'('",
    "')'",
    "'='",
  ];
  private static readonly _SYMBOLIC_NAMES: Array<string | undefined> = [
    undefined,
    'AND',
    'OR',
    'NOT',
    'STAR',
    'PLUS',
    'COLON',
    'LPAREN',
    'RPAREN',
    'EQUAL',
    'QUOTED_STRING',
    'INCOMPLETE_LEFT_QUOTED_STRING',
    'INCOMPLETE_RIGHT_QUOTED_STRING',
    'IDENTIFIER',
    'WS',
  ];
  public static readonly VOCABULARY: Vocabulary = new VocabularyImpl(
    SelectionAutoCompleteParser._LITERAL_NAMES,
    SelectionAutoCompleteParser._SYMBOLIC_NAMES,
    [],
  );

  // @Override
  // @NotNull
  public get vocabulary(): Vocabulary {
    return SelectionAutoCompleteParser.VOCABULARY;
  }
  // tslint:enable:no-trailing-whitespace

  // @Override
  public get grammarFileName(): string {
    return 'SelectionAutoComplete.g4';
  }

  // @Override
  public get ruleNames(): string[] {
    return SelectionAutoCompleteParser.ruleNames;
  }

  // @Override
  public get serializedATN(): string {
    return SelectionAutoCompleteParser._serializedATN;
  }

  protected createFailedPredicateException(
    predicate?: string,
    message?: string,
  ): FailedPredicateException {
    return new FailedPredicateException(this, predicate, message);
  }

  constructor(input: TokenStream) {
    super(input);
    this._interp = new ParserATNSimulator(SelectionAutoCompleteParser._ATN, this);
  }
  // @RuleVersion(0)
  public start(): StartContext {
    const _localctx: StartContext = new StartContext(this._ctx, this.state);
    this.enterRule(_localctx, 0, SelectionAutoCompleteParser.RULE_start);
    try {
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 54;
        this.expr(0);
        this.state = 55;
        this.match(SelectionAutoCompleteParser.EOF);
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }

  public expr(): ExprContext;
  public expr(_p: number): ExprContext;
  // @RuleVersion(0)
  public expr(_p?: number): ExprContext {
    if (_p === undefined) {
      _p = 0;
    }

    const _parentctx: ParserRuleContext = this._ctx;
    const _parentState: number = this.state;
    let _localctx: ExprContext = new ExprContext(this._ctx, _parentState);
    let _prevctx: ExprContext = _localctx;
    const _startState: number = 2;
    this.enterRecursionRule(_localctx, 2, SelectionAutoCompleteParser.RULE_expr, _p);
    try {
      let _alt: number;
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 81;
        this._errHandler.sync(this);
        switch (this.interpreter.adaptivePredict(this._input, 0, this._ctx)) {
          case 1:
            {
              _localctx = new TraversalAllowedExpressionContext(_localctx);
              this._ctx = _localctx;
              _prevctx = _localctx;

              this.state = 58;
              this.traversalAllowedExpr();
            }
            break;

          case 2:
            {
              _localctx = new UpAndDownTraversalExpressionContext(_localctx);
              this._ctx = _localctx;
              _prevctx = _localctx;
              this.state = 59;
              this.upTraversalExpr();
              this.state = 60;
              this.traversalAllowedExpr();
              this.state = 61;
              this.downTraversalExpr();
            }
            break;

          case 3:
            {
              _localctx = new UpTraversalExpressionContext(_localctx);
              this._ctx = _localctx;
              _prevctx = _localctx;
              this.state = 63;
              this.upTraversalExpr();
              this.state = 64;
              this.traversalAllowedExpr();
            }
            break;

          case 4:
            {
              _localctx = new DownTraversalExpressionContext(_localctx);
              this._ctx = _localctx;
              _prevctx = _localctx;
              this.state = 66;
              this.traversalAllowedExpr();
              this.state = 67;
              this.downTraversalExpr();
            }
            break;

          case 5:
            {
              _localctx = new NotExpressionContext(_localctx);
              this._ctx = _localctx;
              _prevctx = _localctx;
              this.state = 69;
              this.notToken();
              this.state = 70;
              this.postNotOperatorWhitespace();
              this.state = 71;
              this.expr(8);
            }
            break;

          case 6:
            {
              _localctx = new IncompleteNotExpressionContext(_localctx);
              this._ctx = _localctx;
              _prevctx = _localctx;
              this.state = 73;
              this.notToken();
              this.state = 74;
              this.postNotOperatorWhitespace();
            }
            break;

          case 7:
            {
              _localctx = new AllExpressionContext(_localctx);
              this._ctx = _localctx;
              _prevctx = _localctx;
              this.state = 76;
              this.match(SelectionAutoCompleteParser.STAR);
              this.state = 77;
              this.postExpressionWhitespace();
            }
            break;

          case 8:
            {
              _localctx = new UnmatchedValueContext(_localctx);
              this._ctx = _localctx;
              _prevctx = _localctx;
              this.state = 78;
              this.value();
              this.state = 79;
              this.postExpressionWhitespace();
            }
            break;
        }
        this._ctx._stop = this._input.tryLT(-1);
        this.state = 103;
        this._errHandler.sync(this);
        _alt = this.interpreter.adaptivePredict(this._input, 2, this._ctx);
        while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
          if (_alt === 1) {
            if (this._parseListeners != null) {
              this.triggerExitRuleEvent();
            }
            _prevctx = _localctx;
            {
              this.state = 101;
              this._errHandler.sync(this);
              switch (this.interpreter.adaptivePredict(this._input, 1, this._ctx)) {
                case 1:
                  {
                    _localctx = new AndExpressionContext(new ExprContext(_parentctx, _parentState));
                    this.pushNewRecursionContext(
                      _localctx,
                      _startState,
                      SelectionAutoCompleteParser.RULE_expr,
                    );
                    this.state = 83;
                    if (!this.precpred(this._ctx, 7)) {
                      throw this.createFailedPredicateException('this.precpred(this._ctx, 7)');
                    }
                    this.state = 84;
                    this.andToken();
                    this.state = 85;
                    this.postLogicalOperatorWhitespace();
                    this.state = 86;
                    this.expr(8);
                  }
                  break;

                case 2:
                  {
                    _localctx = new OrExpressionContext(new ExprContext(_parentctx, _parentState));
                    this.pushNewRecursionContext(
                      _localctx,
                      _startState,
                      SelectionAutoCompleteParser.RULE_expr,
                    );
                    this.state = 88;
                    if (!this.precpred(this._ctx, 6)) {
                      throw this.createFailedPredicateException('this.precpred(this._ctx, 6)');
                    }
                    this.state = 89;
                    this.orToken();
                    this.state = 90;
                    this.postLogicalOperatorWhitespace();
                    this.state = 91;
                    this.expr(7);
                  }
                  break;

                case 3:
                  {
                    _localctx = new IncompleteAndExpressionContext(
                      new ExprContext(_parentctx, _parentState),
                    );
                    this.pushNewRecursionContext(
                      _localctx,
                      _startState,
                      SelectionAutoCompleteParser.RULE_expr,
                    );
                    this.state = 93;
                    if (!this.precpred(this._ctx, 5)) {
                      throw this.createFailedPredicateException('this.precpred(this._ctx, 5)');
                    }
                    this.state = 94;
                    this.andToken();
                    this.state = 95;
                    this.postLogicalOperatorWhitespace();
                  }
                  break;

                case 4:
                  {
                    _localctx = new IncompleteOrExpressionContext(
                      new ExprContext(_parentctx, _parentState),
                    );
                    this.pushNewRecursionContext(
                      _localctx,
                      _startState,
                      SelectionAutoCompleteParser.RULE_expr,
                    );
                    this.state = 97;
                    if (!this.precpred(this._ctx, 4)) {
                      throw this.createFailedPredicateException('this.precpred(this._ctx, 4)');
                    }
                    this.state = 98;
                    this.orToken();
                    this.state = 99;
                    this.postLogicalOperatorWhitespace();
                  }
                  break;
              }
            }
          }
          this.state = 105;
          this._errHandler.sync(this);
          _alt = this.interpreter.adaptivePredict(this._input, 2, this._ctx);
        }
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public traversalAllowedExpr(): TraversalAllowedExprContext {
    let _localctx: TraversalAllowedExprContext = new TraversalAllowedExprContext(
      this._ctx,
      this.state,
    );
    this.enterRule(_localctx, 4, SelectionAutoCompleteParser.RULE_traversalAllowedExpr);
    try {
      this.state = 116;
      this._errHandler.sync(this);
      switch (this.interpreter.adaptivePredict(this._input, 3, this._ctx)) {
        case 1:
          _localctx = new AttributeExpressionContext(_localctx);
          this.enterOuterAlt(_localctx, 1);
          {
            this.state = 106;
            this.attributeName();
            this.state = 107;
            this.colonToken();
            this.state = 108;
            this.attributeValue();
            this.state = 109;
            this.postAttributeValueWhitespace();
          }
          break;

        case 2:
          _localctx = new FunctionCallExpressionContext(_localctx);
          this.enterOuterAlt(_localctx, 2);
          {
            this.state = 111;
            this.functionName();
            this.state = 112;
            this.parenthesizedExpr();
          }
          break;

        case 3:
          _localctx = new TraversalAllowedParenthesizedExpressionContext(_localctx);
          this.enterOuterAlt(_localctx, 3);
          {
            this.state = 114;
            this.parenthesizedExpr();
          }
          break;

        case 4:
          _localctx = new IncompleteExpressionContext(_localctx);
          this.enterOuterAlt(_localctx, 4);
          {
            this.state = 115;
            this.incompleteExpr();
          }
          break;
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public parenthesizedExpr(): ParenthesizedExprContext {
    let _localctx: ParenthesizedExprContext = new ParenthesizedExprContext(this._ctx, this.state);
    this.enterRule(_localctx, 6, SelectionAutoCompleteParser.RULE_parenthesizedExpr);
    try {
      _localctx = new ParenthesizedExpressionContext(_localctx);
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 118;
        this.leftParenToken();
        this.state = 119;
        this.postLogicalOperatorWhitespace();
        this.state = 120;
        this.expr(0);
        this.state = 121;
        this.rightParenToken();
        this.state = 122;
        this.postExpressionWhitespace();
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public incompleteExpr(): IncompleteExprContext {
    let _localctx: IncompleteExprContext = new IncompleteExprContext(this._ctx, this.state);
    this.enterRule(_localctx, 8, SelectionAutoCompleteParser.RULE_incompleteExpr);
    try {
      let _alt: number;
      this.state = 157;
      this._errHandler.sync(this);
      switch (this.interpreter.adaptivePredict(this._input, 5, this._ctx)) {
        case 1:
          _localctx = new IncompleteAttributeExpressionMissingValueContext(_localctx);
          this.enterOuterAlt(_localctx, 1);
          {
            this.state = 124;
            this.attributeName();
            this.state = 125;
            this.colonToken();
            this.state = 126;
            this.attributeValueWhitespace();
          }
          break;

        case 2:
          _localctx = new ExpressionlessFunctionExpressionContext(_localctx);
          this.enterOuterAlt(_localctx, 2);
          {
            this.state = 128;
            this.functionName();
            this.state = 129;
            this.expressionLessParenthesizedExpr();
          }
          break;

        case 3:
          _localctx = new UnclosedExpressionlessFunctionExpressionContext(_localctx);
          this.enterOuterAlt(_localctx, 3);
          {
            this.state = 131;
            this.functionName();
            this.state = 132;
            this.leftParenToken();
            this.state = 133;
            this.postLogicalOperatorWhitespace();
          }
          break;

        case 4:
          _localctx = new UnclosedFunctionExpressionContext(_localctx);
          this.enterOuterAlt(_localctx, 4);
          {
            this.state = 135;
            this.functionName();
            this.state = 136;
            this.leftParenToken();
            this.state = 137;
            this.expr(0);
          }
          break;

        case 5:
          _localctx = new UnclosedParenthesizedExpressionContext(_localctx);
          this.enterOuterAlt(_localctx, 5);
          {
            this.state = 139;
            this.leftParenToken();
            this.state = 140;
            this.postLogicalOperatorWhitespace();
            this.state = 141;
            this.expr(0);
          }
          break;

        case 6:
          _localctx = new ExpressionlessParenthesizedExpressionWrapperContext(_localctx);
          this.enterOuterAlt(_localctx, 6);
          {
            this.state = 143;
            this.expressionLessParenthesizedExpr();
          }
          break;

        case 7:
          _localctx = new UnclosedExpressionlessParenthesizedExpressionContext(_localctx);
          this.enterOuterAlt(_localctx, 7);
          {
            this.state = 144;
            this.leftParenToken();
            this.state = 145;
            this.postLogicalOperatorWhitespace();
          }
          break;

        case 8:
          _localctx = new IncompletePlusTraversalExpressionContext(_localctx);
          this.enterOuterAlt(_localctx, 8);
          {
            this.state = 148;
            this._errHandler.sync(this);
            _alt = 1;
            do {
              switch (_alt) {
                case 1:
                  {
                    {
                      this.state = 147;
                      this.match(SelectionAutoCompleteParser.PLUS);
                    }
                  }
                  break;
                default:
                  throw new NoViableAltException(this);
              }
              this.state = 150;
              this._errHandler.sync(this);
              _alt = this.interpreter.adaptivePredict(this._input, 4, this._ctx);
            } while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER);
            this.state = 152;
            this.postNeighborTraversalWhitespace();
          }
          break;

        case 9:
          _localctx = new IncompleteAttributeExpressionMissingKeyContext(_localctx);
          this.enterOuterAlt(_localctx, 9);
          {
            this.state = 153;
            this.colonToken();
            this.state = 154;
            this.attributeValue();
            this.state = 155;
            this.postExpressionWhitespace();
          }
          break;
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public expressionLessParenthesizedExpr(): ExpressionLessParenthesizedExprContext {
    let _localctx: ExpressionLessParenthesizedExprContext =
      new ExpressionLessParenthesizedExprContext(this._ctx, this.state);
    this.enterRule(_localctx, 10, SelectionAutoCompleteParser.RULE_expressionLessParenthesizedExpr);
    try {
      _localctx = new ExpressionlessParenthesizedExpressionContext(_localctx);
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 159;
        this.leftParenToken();
        this.state = 160;
        this.postLogicalOperatorWhitespace();
        this.state = 161;
        this.rightParenToken();
        this.state = 162;
        this.postExpressionWhitespace();
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public upTraversalExpr(): UpTraversalExprContext {
    let _localctx: UpTraversalExprContext = new UpTraversalExprContext(this._ctx, this.state);
    this.enterRule(_localctx, 12, SelectionAutoCompleteParser.RULE_upTraversalExpr);
    try {
      _localctx = new UpTraversalContext(_localctx);
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 164;
        this.traversal();
        this.state = 165;
        this.postUpwardTraversalWhitespace();
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public downTraversalExpr(): DownTraversalExprContext {
    let _localctx: DownTraversalExprContext = new DownTraversalExprContext(this._ctx, this.state);
    this.enterRule(_localctx, 14, SelectionAutoCompleteParser.RULE_downTraversalExpr);
    try {
      _localctx = new DownTraversalContext(_localctx);
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 167;
        this.traversal();
        this.state = 168;
        this.postDownwardTraversalWhitespace();
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public traversal(): TraversalContext {
    const _localctx: TraversalContext = new TraversalContext(this._ctx, this.state);
    this.enterRule(_localctx, 16, SelectionAutoCompleteParser.RULE_traversal);
    try {
      let _alt: number;
      this.state = 176;
      this._errHandler.sync(this);
      switch (this._input.LA(1)) {
        case SelectionAutoCompleteParser.STAR:
          this.enterOuterAlt(_localctx, 1);
          {
            this.state = 170;
            this.match(SelectionAutoCompleteParser.STAR);
          }
          break;
        case SelectionAutoCompleteParser.PLUS:
          this.enterOuterAlt(_localctx, 2);
          {
            this.state = 172;
            this._errHandler.sync(this);
            _alt = 1;
            do {
              switch (_alt) {
                case 1:
                  {
                    {
                      this.state = 171;
                      this.match(SelectionAutoCompleteParser.PLUS);
                    }
                  }
                  break;
                default:
                  throw new NoViableAltException(this);
              }
              this.state = 174;
              this._errHandler.sync(this);
              _alt = this.interpreter.adaptivePredict(this._input, 6, this._ctx);
            } while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER);
          }
          break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public attributeName(): AttributeNameContext {
    const _localctx: AttributeNameContext = new AttributeNameContext(this._ctx, this.state);
    this.enterRule(_localctx, 18, SelectionAutoCompleteParser.RULE_attributeName);
    try {
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 178;
        this.match(SelectionAutoCompleteParser.IDENTIFIER);
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public attributeValue(): AttributeValueContext {
    const _localctx: AttributeValueContext = new AttributeValueContext(this._ctx, this.state);
    this.enterRule(_localctx, 20, SelectionAutoCompleteParser.RULE_attributeValue);
    try {
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 180;
        this.value();
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public functionName(): FunctionNameContext {
    const _localctx: FunctionNameContext = new FunctionNameContext(this._ctx, this.state);
    this.enterRule(_localctx, 22, SelectionAutoCompleteParser.RULE_functionName);
    try {
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 182;
        this.match(SelectionAutoCompleteParser.IDENTIFIER);
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public orToken(): OrTokenContext {
    const _localctx: OrTokenContext = new OrTokenContext(this._ctx, this.state);
    this.enterRule(_localctx, 24, SelectionAutoCompleteParser.RULE_orToken);
    try {
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 184;
        this.match(SelectionAutoCompleteParser.OR);
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public andToken(): AndTokenContext {
    const _localctx: AndTokenContext = new AndTokenContext(this._ctx, this.state);
    this.enterRule(_localctx, 26, SelectionAutoCompleteParser.RULE_andToken);
    try {
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 186;
        this.match(SelectionAutoCompleteParser.AND);
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public notToken(): NotTokenContext {
    const _localctx: NotTokenContext = new NotTokenContext(this._ctx, this.state);
    this.enterRule(_localctx, 28, SelectionAutoCompleteParser.RULE_notToken);
    try {
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 188;
        this.match(SelectionAutoCompleteParser.NOT);
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public colonToken(): ColonTokenContext {
    const _localctx: ColonTokenContext = new ColonTokenContext(this._ctx, this.state);
    this.enterRule(_localctx, 30, SelectionAutoCompleteParser.RULE_colonToken);
    try {
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 190;
        this.match(SelectionAutoCompleteParser.COLON);
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public leftParenToken(): LeftParenTokenContext {
    const _localctx: LeftParenTokenContext = new LeftParenTokenContext(this._ctx, this.state);
    this.enterRule(_localctx, 32, SelectionAutoCompleteParser.RULE_leftParenToken);
    try {
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 192;
        this.match(SelectionAutoCompleteParser.LPAREN);
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public rightParenToken(): RightParenTokenContext {
    const _localctx: RightParenTokenContext = new RightParenTokenContext(this._ctx, this.state);
    this.enterRule(_localctx, 34, SelectionAutoCompleteParser.RULE_rightParenToken);
    try {
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 194;
        this.match(SelectionAutoCompleteParser.RPAREN);
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public attributeValueWhitespace(): AttributeValueWhitespaceContext {
    const _localctx: AttributeValueWhitespaceContext = new AttributeValueWhitespaceContext(
      this._ctx,
      this.state,
    );
    this.enterRule(_localctx, 36, SelectionAutoCompleteParser.RULE_attributeValueWhitespace);
    try {
      let _alt: number;
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 199;
        this._errHandler.sync(this);
        _alt = this.interpreter.adaptivePredict(this._input, 8, this._ctx);
        while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
          if (_alt === 1) {
            {
              {
                this.state = 196;
                this.match(SelectionAutoCompleteParser.WS);
              }
            }
          }
          this.state = 201;
          this._errHandler.sync(this);
          _alt = this.interpreter.adaptivePredict(this._input, 8, this._ctx);
        }
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public postAttributeValueWhitespace(): PostAttributeValueWhitespaceContext {
    const _localctx: PostAttributeValueWhitespaceContext = new PostAttributeValueWhitespaceContext(
      this._ctx,
      this.state,
    );
    this.enterRule(_localctx, 38, SelectionAutoCompleteParser.RULE_postAttributeValueWhitespace);
    try {
      let _alt: number;
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 205;
        this._errHandler.sync(this);
        _alt = this.interpreter.adaptivePredict(this._input, 9, this._ctx);
        while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
          if (_alt === 1) {
            {
              {
                this.state = 202;
                this.match(SelectionAutoCompleteParser.WS);
              }
            }
          }
          this.state = 207;
          this._errHandler.sync(this);
          _alt = this.interpreter.adaptivePredict(this._input, 9, this._ctx);
        }
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public postExpressionWhitespace(): PostExpressionWhitespaceContext {
    const _localctx: PostExpressionWhitespaceContext = new PostExpressionWhitespaceContext(
      this._ctx,
      this.state,
    );
    this.enterRule(_localctx, 40, SelectionAutoCompleteParser.RULE_postExpressionWhitespace);
    try {
      let _alt: number;
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 211;
        this._errHandler.sync(this);
        _alt = this.interpreter.adaptivePredict(this._input, 10, this._ctx);
        while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
          if (_alt === 1) {
            {
              {
                this.state = 208;
                this.match(SelectionAutoCompleteParser.WS);
              }
            }
          }
          this.state = 213;
          this._errHandler.sync(this);
          _alt = this.interpreter.adaptivePredict(this._input, 10, this._ctx);
        }
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public postNotOperatorWhitespace(): PostNotOperatorWhitespaceContext {
    const _localctx: PostNotOperatorWhitespaceContext = new PostNotOperatorWhitespaceContext(
      this._ctx,
      this.state,
    );
    this.enterRule(_localctx, 42, SelectionAutoCompleteParser.RULE_postNotOperatorWhitespace);
    try {
      let _alt: number;
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 217;
        this._errHandler.sync(this);
        _alt = this.interpreter.adaptivePredict(this._input, 11, this._ctx);
        while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
          if (_alt === 1) {
            {
              {
                this.state = 214;
                this.match(SelectionAutoCompleteParser.WS);
              }
            }
          }
          this.state = 219;
          this._errHandler.sync(this);
          _alt = this.interpreter.adaptivePredict(this._input, 11, this._ctx);
        }
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public postLogicalOperatorWhitespace(): PostLogicalOperatorWhitespaceContext {
    const _localctx: PostLogicalOperatorWhitespaceContext =
      new PostLogicalOperatorWhitespaceContext(this._ctx, this.state);
    this.enterRule(_localctx, 44, SelectionAutoCompleteParser.RULE_postLogicalOperatorWhitespace);
    try {
      let _alt: number;
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 223;
        this._errHandler.sync(this);
        _alt = this.interpreter.adaptivePredict(this._input, 12, this._ctx);
        while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
          if (_alt === 1) {
            {
              {
                this.state = 220;
                this.match(SelectionAutoCompleteParser.WS);
              }
            }
          }
          this.state = 225;
          this._errHandler.sync(this);
          _alt = this.interpreter.adaptivePredict(this._input, 12, this._ctx);
        }
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public postNeighborTraversalWhitespace(): PostNeighborTraversalWhitespaceContext {
    const _localctx: PostNeighborTraversalWhitespaceContext =
      new PostNeighborTraversalWhitespaceContext(this._ctx, this.state);
    this.enterRule(_localctx, 46, SelectionAutoCompleteParser.RULE_postNeighborTraversalWhitespace);
    try {
      let _alt: number;
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 229;
        this._errHandler.sync(this);
        _alt = this.interpreter.adaptivePredict(this._input, 13, this._ctx);
        while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
          if (_alt === 1) {
            {
              {
                this.state = 226;
                this.match(SelectionAutoCompleteParser.WS);
              }
            }
          }
          this.state = 231;
          this._errHandler.sync(this);
          _alt = this.interpreter.adaptivePredict(this._input, 13, this._ctx);
        }
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public postUpwardTraversalWhitespace(): PostUpwardTraversalWhitespaceContext {
    const _localctx: PostUpwardTraversalWhitespaceContext =
      new PostUpwardTraversalWhitespaceContext(this._ctx, this.state);
    this.enterRule(_localctx, 48, SelectionAutoCompleteParser.RULE_postUpwardTraversalWhitespace);
    let _la: number;
    try {
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 235;
        this._errHandler.sync(this);
        _la = this._input.LA(1);
        while (_la === SelectionAutoCompleteParser.WS) {
          {
            {
              this.state = 232;
              this.match(SelectionAutoCompleteParser.WS);
            }
          }
          this.state = 237;
          this._errHandler.sync(this);
          _la = this._input.LA(1);
        }
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public postDownwardTraversalWhitespace(): PostDownwardTraversalWhitespaceContext {
    const _localctx: PostDownwardTraversalWhitespaceContext =
      new PostDownwardTraversalWhitespaceContext(this._ctx, this.state);
    this.enterRule(_localctx, 50, SelectionAutoCompleteParser.RULE_postDownwardTraversalWhitespace);
    try {
      let _alt: number;
      this.enterOuterAlt(_localctx, 1);
      {
        this.state = 241;
        this._errHandler.sync(this);
        _alt = this.interpreter.adaptivePredict(this._input, 15, this._ctx);
        while (_alt !== 2 && _alt !== ATN.INVALID_ALT_NUMBER) {
          if (_alt === 1) {
            {
              {
                this.state = 238;
                this.match(SelectionAutoCompleteParser.WS);
              }
            }
          }
          this.state = 243;
          this._errHandler.sync(this);
          _alt = this.interpreter.adaptivePredict(this._input, 15, this._ctx);
        }
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }
  // @RuleVersion(0)
  public value(): ValueContext {
    let _localctx: ValueContext = new ValueContext(this._ctx, this.state);
    this.enterRule(_localctx, 52, SelectionAutoCompleteParser.RULE_value);
    try {
      this.state = 248;
      this._errHandler.sync(this);
      switch (this._input.LA(1)) {
        case SelectionAutoCompleteParser.QUOTED_STRING:
          _localctx = new QuotedStringValueContext(_localctx);
          this.enterOuterAlt(_localctx, 1);
          {
            this.state = 244;
            this.match(SelectionAutoCompleteParser.QUOTED_STRING);
          }
          break;
        case SelectionAutoCompleteParser.INCOMPLETE_LEFT_QUOTED_STRING:
          _localctx = new IncompleteLeftQuotedStringValueContext(_localctx);
          this.enterOuterAlt(_localctx, 2);
          {
            this.state = 245;
            this.match(SelectionAutoCompleteParser.INCOMPLETE_LEFT_QUOTED_STRING);
          }
          break;
        case SelectionAutoCompleteParser.INCOMPLETE_RIGHT_QUOTED_STRING:
          _localctx = new IncompleteRightQuotedStringValueContext(_localctx);
          this.enterOuterAlt(_localctx, 3);
          {
            this.state = 246;
            this.match(SelectionAutoCompleteParser.INCOMPLETE_RIGHT_QUOTED_STRING);
          }
          break;
        case SelectionAutoCompleteParser.IDENTIFIER:
          _localctx = new UnquotedStringValueContext(_localctx);
          this.enterOuterAlt(_localctx, 4);
          {
            this.state = 247;
            this.match(SelectionAutoCompleteParser.IDENTIFIER);
          }
          break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (re) {
      if (re instanceof RecognitionException) {
        _localctx.exception = re;
        this._errHandler.reportError(this, re);
        this._errHandler.recover(this, re);
      } else {
        throw re;
      }
    } finally {
      this.exitRule();
    }
    return _localctx;
  }

  public sempred(_localctx: RuleContext, ruleIndex: number, predIndex: number): boolean {
    switch (ruleIndex) {
      case 1:
        return this.expr_sempred(_localctx as ExprContext, predIndex);
    }
    return true;
  }
  private expr_sempred(_localctx: ExprContext, predIndex: number): boolean {
    switch (predIndex) {
      case 0:
        return this.precpred(this._ctx, 7);

      case 1:
        return this.precpred(this._ctx, 6);

      case 2:
        return this.precpred(this._ctx, 5);

      case 3:
        return this.precpred(this._ctx, 4);
    }
    return true;
  }

  public static readonly _serializedATN: string =
    '\x03\uC91D\uCABA\u058D\uAFBA\u4F53\u0607\uEA8B\uC241\x03\x10\xFD\x04\x02' +
    '\t\x02\x04\x03\t\x03\x04\x04\t\x04\x04\x05\t\x05\x04\x06\t\x06\x04\x07' +
    '\t\x07\x04\b\t\b\x04\t\t\t\x04\n\t\n\x04\v\t\v\x04\f\t\f\x04\r\t\r\x04' +
    '\x0E\t\x0E\x04\x0F\t\x0F\x04\x10\t\x10\x04\x11\t\x11\x04\x12\t\x12\x04' +
    '\x13\t\x13\x04\x14\t\x14\x04\x15\t\x15\x04\x16\t\x16\x04\x17\t\x17\x04' +
    '\x18\t\x18\x04\x19\t\x19\x04\x1A\t\x1A\x04\x1B\t\x1B\x04\x1C\t\x1C\x03' +
    '\x02\x03\x02\x03\x02\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03' +
    '\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03' +
    '\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x05' +
    '\x03T\n\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03' +
    '\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03' +
    '\x03\x03\x03\x07\x03h\n\x03\f\x03\x0E\x03k\v\x03\x03\x04\x03\x04\x03\x04' +
    '\x03\x04\x03\x04\x03\x04\x03\x04\x03\x04\x03\x04\x03\x04\x05\x04w\n\x04' +
    '\x03\x05\x03\x05\x03\x05\x03\x05\x03\x05\x03\x05\x03\x06\x03\x06\x03\x06' +
    '\x03\x06\x03\x06\x03\x06\x03\x06\x03\x06\x03\x06\x03\x06\x03\x06\x03\x06' +
    '\x03\x06\x03\x06\x03\x06\x03\x06\x03\x06\x03\x06\x03\x06\x03\x06\x03\x06' +
    '\x03\x06\x03\x06\x03\x06\x06\x06\x97\n\x06\r\x06\x0E\x06\x98\x03\x06\x03' +
    '\x06\x03\x06\x03\x06\x03\x06\x05\x06\xA0\n\x06\x03\x07\x03\x07\x03\x07' +
    '\x03\x07\x03\x07\x03\b\x03\b\x03\b\x03\t\x03\t\x03\t\x03\n\x03\n\x06\n' +
    '\xAF\n\n\r\n\x0E\n\xB0\x05\n\xB3\n\n\x03\v\x03\v\x03\f\x03\f\x03\r\x03' +
    '\r\x03\x0E\x03\x0E\x03\x0F\x03\x0F\x03\x10\x03\x10\x03\x11\x03\x11\x03' +
    '\x12\x03\x12\x03\x13\x03\x13\x03\x14\x07\x14\xC8\n\x14\f\x14\x0E\x14\xCB' +
    '\v\x14\x03\x15\x07\x15\xCE\n\x15\f\x15\x0E\x15\xD1\v\x15\x03\x16\x07\x16' +
    '\xD4\n\x16\f\x16\x0E\x16\xD7\v\x16\x03\x17\x07\x17\xDA\n\x17\f\x17\x0E' +
    '\x17\xDD\v\x17\x03\x18\x07\x18\xE0\n\x18\f\x18\x0E\x18\xE3\v\x18\x03\x19' +
    '\x07\x19\xE6\n\x19\f\x19\x0E\x19\xE9\v\x19\x03\x1A\x07\x1A\xEC\n\x1A\f' +
    '\x1A\x0E\x1A\xEF\v\x1A\x03\x1B\x07\x1B\xF2\n\x1B\f\x1B\x0E\x1B\xF5\v\x1B' +
    '\x03\x1C\x03\x1C\x03\x1C\x03\x1C\x05\x1C\xFB\n\x1C\x03\x1C\x02\x02\x03' +
    '\x04\x1D\x02\x02\x04\x02\x06\x02\b\x02\n\x02\f\x02\x0E\x02\x10\x02\x12' +
    '\x02\x14\x02\x16\x02\x18\x02\x1A\x02\x1C\x02\x1E\x02 \x02"\x02$\x02&' +
    '\x02(\x02*\x02,\x02.\x020\x022\x024\x026\x02\x02\x02\x02\u0105\x028\x03' +
    '\x02\x02\x02\x04S\x03\x02\x02\x02\x06v\x03\x02\x02\x02\bx\x03\x02\x02' +
    '\x02\n\x9F\x03\x02\x02\x02\f\xA1\x03\x02\x02\x02\x0E\xA6\x03\x02\x02\x02' +
    '\x10\xA9\x03\x02\x02\x02\x12\xB2\x03\x02\x02\x02\x14\xB4\x03\x02\x02\x02' +
    '\x16\xB6\x03\x02\x02\x02\x18\xB8\x03\x02\x02\x02\x1A\xBA\x03\x02\x02\x02' +
    '\x1C\xBC\x03\x02\x02\x02\x1E\xBE\x03\x02\x02\x02 \xC0\x03\x02\x02\x02' +
    '"\xC2\x03\x02\x02\x02$\xC4\x03\x02\x02\x02&\xC9\x03\x02\x02\x02(\xCF' +
    '\x03\x02\x02\x02*\xD5\x03\x02\x02\x02,\xDB\x03\x02\x02\x02.\xE1\x03\x02' +
    '\x02\x020\xE7\x03\x02\x02\x022\xED\x03\x02\x02\x024\xF3\x03\x02\x02\x02' +
    '6\xFA\x03\x02\x02\x0289\x05\x04\x03\x029:\x07\x02\x02\x03:\x03\x03\x02' +
    '\x02\x02;<\b\x03\x01\x02<T\x05\x06\x04\x02=>\x05\x0E\b\x02>?\x05\x06\x04' +
    '\x02?@\x05\x10\t\x02@T\x03\x02\x02\x02AB\x05\x0E\b\x02BC\x05\x06\x04\x02' +
    'CT\x03\x02\x02\x02DE\x05\x06\x04\x02EF\x05\x10\t\x02FT\x03\x02\x02\x02' +
    'GH\x05\x1E\x10\x02HI\x05,\x17\x02IJ\x05\x04\x03\nJT\x03\x02\x02\x02KL' +
    '\x05\x1E\x10\x02LM\x05,\x17\x02MT\x03\x02\x02\x02NO\x07\x06\x02\x02OT' +
    '\x05*\x16\x02PQ\x056\x1C\x02QR\x05*\x16\x02RT\x03\x02\x02\x02S;\x03\x02' +
    '\x02\x02S=\x03\x02\x02\x02SA\x03\x02\x02\x02SD\x03\x02\x02\x02SG\x03\x02' +
    '\x02\x02SK\x03\x02\x02\x02SN\x03\x02\x02\x02SP\x03\x02\x02\x02Ti\x03\x02' +
    '\x02\x02UV\f\t\x02\x02VW\x05\x1C\x0F\x02WX\x05.\x18\x02XY\x05\x04\x03' +
    '\nYh\x03\x02\x02\x02Z[\f\b\x02\x02[\\\x05\x1A\x0E\x02\\]\x05.\x18\x02' +
    ']^\x05\x04\x03\t^h\x03\x02\x02\x02_`\f\x07\x02\x02`a\x05\x1C\x0F\x02a' +
    'b\x05.\x18\x02bh\x03\x02\x02\x02cd\f\x06\x02\x02de\x05\x1A\x0E\x02ef\x05' +
    '.\x18\x02fh\x03\x02\x02\x02gU\x03\x02\x02\x02gZ\x03\x02\x02\x02g_\x03' +
    '\x02\x02\x02gc\x03\x02\x02\x02hk\x03\x02\x02\x02ig\x03\x02\x02\x02ij\x03' +
    '\x02\x02\x02j\x05\x03\x02\x02\x02ki\x03\x02\x02\x02lm\x05\x14\v\x02mn' +
    '\x05 \x11\x02no\x05\x16\f\x02op\x05(\x15\x02pw\x03\x02\x02\x02qr\x05\x18' +
    '\r\x02rs\x05\b\x05\x02sw\x03\x02\x02\x02tw\x05\b\x05\x02uw\x05\n\x06\x02' +
    'vl\x03\x02\x02\x02vq\x03\x02\x02\x02vt\x03\x02\x02\x02vu\x03\x02\x02\x02' +
    'w\x07\x03\x02\x02\x02xy\x05"\x12\x02yz\x05.\x18\x02z{\x05\x04\x03\x02' +
    '{|\x05$\x13\x02|}\x05*\x16\x02}\t\x03\x02\x02\x02~\x7F\x05\x14\v\x02\x7F' +
    '\x80\x05 \x11\x02\x80\x81\x05&\x14\x02\x81\xA0\x03\x02\x02\x02\x82\x83' +
    '\x05\x18\r\x02\x83\x84\x05\f\x07\x02\x84\xA0\x03\x02\x02\x02\x85\x86\x05' +
    '\x18\r\x02\x86\x87\x05"\x12\x02\x87\x88\x05.\x18\x02\x88\xA0\x03\x02' +
    '\x02\x02\x89\x8A\x05\x18\r\x02\x8A\x8B\x05"\x12\x02\x8B\x8C\x05\x04\x03' +
    '\x02\x8C\xA0\x03\x02\x02\x02\x8D\x8E\x05"\x12\x02\x8E\x8F\x05.\x18\x02' +
    '\x8F\x90\x05\x04\x03\x02\x90\xA0\x03\x02\x02\x02\x91\xA0\x05\f\x07\x02' +
    '\x92\x93\x05"\x12\x02\x93\x94\x05.\x18\x02\x94\xA0\x03\x02\x02\x02\x95' +
    '\x97\x07\x07\x02\x02\x96\x95\x03\x02\x02\x02\x97\x98\x03\x02\x02\x02\x98' +
    '\x96\x03\x02\x02\x02\x98\x99\x03\x02\x02\x02\x99\x9A\x03\x02\x02\x02\x9A' +
    '\xA0\x050\x19\x02\x9B\x9C\x05 \x11\x02\x9C\x9D\x05\x16\f\x02\x9D\x9E\x05' +
    '*\x16\x02\x9E\xA0\x03\x02\x02\x02\x9F~\x03\x02\x02\x02\x9F\x82\x03\x02' +
    '\x02\x02\x9F\x85\x03\x02\x02\x02\x9F\x89\x03\x02\x02\x02\x9F\x8D\x03\x02' +
    '\x02\x02\x9F\x91\x03\x02\x02\x02\x9F\x92\x03\x02\x02\x02\x9F\x96\x03\x02' +
    '\x02\x02\x9F\x9B\x03\x02\x02\x02\xA0\v\x03\x02\x02\x02\xA1\xA2\x05"\x12' +
    '\x02\xA2\xA3\x05.\x18\x02\xA3\xA4\x05$\x13\x02\xA4\xA5\x05*\x16\x02\xA5' +
    '\r\x03\x02\x02\x02\xA6\xA7\x05\x12\n\x02\xA7\xA8\x052\x1A\x02\xA8\x0F' +
    '\x03\x02\x02\x02\xA9\xAA\x05\x12\n\x02\xAA\xAB\x054\x1B\x02\xAB\x11\x03' +
    '\x02\x02\x02\xAC\xB3\x07\x06\x02\x02\xAD\xAF\x07\x07\x02\x02\xAE\xAD\x03' +
    '\x02\x02\x02\xAF\xB0\x03\x02\x02\x02\xB0\xAE\x03\x02\x02\x02\xB0\xB1\x03' +
    '\x02\x02\x02\xB1\xB3\x03\x02\x02\x02\xB2\xAC\x03\x02\x02\x02\xB2\xAE\x03' +
    '\x02\x02\x02\xB3\x13\x03\x02\x02\x02\xB4\xB5\x07\x0F\x02\x02\xB5\x15\x03' +
    '\x02\x02\x02\xB6\xB7\x056\x1C\x02\xB7\x17\x03\x02\x02\x02\xB8\xB9\x07' +
    '\x0F\x02\x02\xB9\x19\x03\x02\x02\x02\xBA\xBB\x07\x04\x02\x02\xBB\x1B\x03' +
    '\x02\x02\x02\xBC\xBD\x07\x03\x02\x02\xBD\x1D\x03\x02\x02\x02\xBE\xBF\x07' +
    '\x05\x02\x02\xBF\x1F\x03\x02\x02\x02\xC0\xC1\x07\b\x02\x02\xC1!\x03\x02' +
    '\x02\x02\xC2\xC3\x07\t\x02\x02\xC3#\x03\x02\x02\x02\xC4\xC5\x07\n\x02' +
    '\x02\xC5%\x03\x02\x02\x02\xC6\xC8\x07\x10\x02\x02\xC7\xC6\x03\x02\x02' +
    '\x02\xC8\xCB\x03\x02\x02\x02\xC9\xC7\x03\x02\x02\x02\xC9\xCA\x03\x02\x02' +
    "\x02\xCA'\x03\x02\x02\x02\xCB\xC9\x03\x02\x02\x02\xCC\xCE\x07\x10\x02" +
    '\x02\xCD\xCC\x03\x02\x02\x02\xCE\xD1\x03\x02\x02\x02\xCF\xCD\x03\x02\x02' +
    '\x02\xCF\xD0\x03\x02\x02\x02\xD0)\x03\x02\x02\x02\xD1\xCF\x03\x02\x02' +
    '\x02\xD2\xD4\x07\x10\x02\x02\xD3\xD2\x03\x02\x02\x02\xD4\xD7\x03\x02\x02' +
    '\x02\xD5\xD3\x03\x02\x02\x02\xD5\xD6\x03\x02\x02\x02\xD6+\x03\x02\x02' +
    '\x02\xD7\xD5\x03\x02\x02\x02\xD8\xDA\x07\x10\x02\x02\xD9\xD8\x03\x02\x02' +
    '\x02\xDA\xDD\x03\x02\x02\x02\xDB\xD9\x03\x02\x02\x02\xDB\xDC\x03\x02\x02' +
    '\x02\xDC-\x03\x02\x02\x02\xDD\xDB\x03\x02\x02\x02\xDE\xE0\x07\x10\x02' +
    '\x02\xDF\xDE\x03\x02\x02\x02\xE0\xE3\x03\x02\x02\x02\xE1\xDF\x03\x02\x02' +
    '\x02\xE1\xE2\x03\x02\x02\x02\xE2/\x03\x02\x02\x02\xE3\xE1\x03\x02\x02' +
    '\x02\xE4\xE6\x07\x10\x02\x02\xE5\xE4\x03\x02\x02\x02\xE6\xE9\x03\x02\x02' +
    '\x02\xE7\xE5\x03\x02\x02\x02\xE7\xE8\x03\x02\x02\x02\xE81\x03\x02\x02' +
    '\x02\xE9\xE7\x03\x02\x02\x02\xEA\xEC\x07\x10\x02\x02\xEB\xEA\x03\x02\x02' +
    '\x02\xEC\xEF\x03\x02\x02\x02\xED\xEB\x03\x02\x02\x02\xED\xEE\x03\x02\x02' +
    '\x02\xEE3\x03\x02\x02\x02\xEF\xED\x03\x02\x02\x02\xF0\xF2\x07\x10\x02' +
    '\x02\xF1\xF0\x03\x02\x02\x02\xF2\xF5\x03\x02\x02\x02\xF3\xF1\x03\x02\x02' +
    '\x02\xF3\xF4\x03\x02\x02\x02\xF45\x03\x02\x02\x02\xF5\xF3\x03\x02\x02' +
    '\x02\xF6\xFB\x07\f\x02\x02\xF7\xFB\x07\r\x02\x02\xF8\xFB\x07\x0E\x02\x02' +
    '\xF9\xFB\x07\x0F\x02\x02\xFA\xF6\x03\x02\x02\x02\xFA\xF7\x03\x02\x02\x02' +
    '\xFA\xF8\x03\x02\x02\x02\xFA\xF9\x03\x02\x02\x02\xFB7\x03\x02\x02\x02' +
    '\x13Sgiv\x98\x9F\xB0\xB2\xC9\xCF\xD5\xDB\xE1\xE7\xED\xF3\xFA';
  public static __ATN: ATN;
  public static get _ATN(): ATN {
    if (!SelectionAutoCompleteParser.__ATN) {
      SelectionAutoCompleteParser.__ATN = new ATNDeserializer().deserialize(
        Utils.toCharArray(SelectionAutoCompleteParser._serializedATN),
      );
    }

    return SelectionAutoCompleteParser.__ATN;
  }
}

export class StartContext extends ParserRuleContext {
  public expr(): ExprContext {
    return this.getRuleContext(0, ExprContext);
  }
  public EOF(): TerminalNode {
    return this.getToken(SelectionAutoCompleteParser.EOF, 0);
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_start;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterStart) {
      listener.enterStart(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitStart) {
      listener.exitStart(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitStart) {
      return visitor.visitStart(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class ExprContext extends ParserRuleContext {
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_expr;
  }
  public copyFrom(ctx: ExprContext): void {
    super.copyFrom(ctx);
  }
}
export class TraversalAllowedExpressionContext extends ExprContext {
  public traversalAllowedExpr(): TraversalAllowedExprContext {
    return this.getRuleContext(0, TraversalAllowedExprContext);
  }
  constructor(ctx: ExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterTraversalAllowedExpression) {
      listener.enterTraversalAllowedExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitTraversalAllowedExpression) {
      listener.exitTraversalAllowedExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitTraversalAllowedExpression) {
      return visitor.visitTraversalAllowedExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class UpAndDownTraversalExpressionContext extends ExprContext {
  public upTraversalExpr(): UpTraversalExprContext {
    return this.getRuleContext(0, UpTraversalExprContext);
  }
  public traversalAllowedExpr(): TraversalAllowedExprContext {
    return this.getRuleContext(0, TraversalAllowedExprContext);
  }
  public downTraversalExpr(): DownTraversalExprContext {
    return this.getRuleContext(0, DownTraversalExprContext);
  }
  constructor(ctx: ExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterUpAndDownTraversalExpression) {
      listener.enterUpAndDownTraversalExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitUpAndDownTraversalExpression) {
      listener.exitUpAndDownTraversalExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitUpAndDownTraversalExpression) {
      return visitor.visitUpAndDownTraversalExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class UpTraversalExpressionContext extends ExprContext {
  public upTraversalExpr(): UpTraversalExprContext {
    return this.getRuleContext(0, UpTraversalExprContext);
  }
  public traversalAllowedExpr(): TraversalAllowedExprContext {
    return this.getRuleContext(0, TraversalAllowedExprContext);
  }
  constructor(ctx: ExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterUpTraversalExpression) {
      listener.enterUpTraversalExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitUpTraversalExpression) {
      listener.exitUpTraversalExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitUpTraversalExpression) {
      return visitor.visitUpTraversalExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class DownTraversalExpressionContext extends ExprContext {
  public traversalAllowedExpr(): TraversalAllowedExprContext {
    return this.getRuleContext(0, TraversalAllowedExprContext);
  }
  public downTraversalExpr(): DownTraversalExprContext {
    return this.getRuleContext(0, DownTraversalExprContext);
  }
  constructor(ctx: ExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterDownTraversalExpression) {
      listener.enterDownTraversalExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitDownTraversalExpression) {
      listener.exitDownTraversalExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitDownTraversalExpression) {
      return visitor.visitDownTraversalExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class NotExpressionContext extends ExprContext {
  public notToken(): NotTokenContext {
    return this.getRuleContext(0, NotTokenContext);
  }
  public postNotOperatorWhitespace(): PostNotOperatorWhitespaceContext {
    return this.getRuleContext(0, PostNotOperatorWhitespaceContext);
  }
  public expr(): ExprContext {
    return this.getRuleContext(0, ExprContext);
  }
  constructor(ctx: ExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterNotExpression) {
      listener.enterNotExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitNotExpression) {
      listener.exitNotExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitNotExpression) {
      return visitor.visitNotExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class AndExpressionContext extends ExprContext {
  public expr(): ExprContext[];
  public expr(i: number): ExprContext;
  public expr(i?: number): ExprContext | ExprContext[] {
    if (i === undefined) {
      return this.getRuleContexts(ExprContext);
    } else {
      return this.getRuleContext(i, ExprContext);
    }
  }
  public andToken(): AndTokenContext {
    return this.getRuleContext(0, AndTokenContext);
  }
  public postLogicalOperatorWhitespace(): PostLogicalOperatorWhitespaceContext {
    return this.getRuleContext(0, PostLogicalOperatorWhitespaceContext);
  }
  constructor(ctx: ExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterAndExpression) {
      listener.enterAndExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitAndExpression) {
      listener.exitAndExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitAndExpression) {
      return visitor.visitAndExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class OrExpressionContext extends ExprContext {
  public expr(): ExprContext[];
  public expr(i: number): ExprContext;
  public expr(i?: number): ExprContext | ExprContext[] {
    if (i === undefined) {
      return this.getRuleContexts(ExprContext);
    } else {
      return this.getRuleContext(i, ExprContext);
    }
  }
  public orToken(): OrTokenContext {
    return this.getRuleContext(0, OrTokenContext);
  }
  public postLogicalOperatorWhitespace(): PostLogicalOperatorWhitespaceContext {
    return this.getRuleContext(0, PostLogicalOperatorWhitespaceContext);
  }
  constructor(ctx: ExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterOrExpression) {
      listener.enterOrExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitOrExpression) {
      listener.exitOrExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitOrExpression) {
      return visitor.visitOrExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class IncompleteAndExpressionContext extends ExprContext {
  public expr(): ExprContext {
    return this.getRuleContext(0, ExprContext);
  }
  public andToken(): AndTokenContext {
    return this.getRuleContext(0, AndTokenContext);
  }
  public postLogicalOperatorWhitespace(): PostLogicalOperatorWhitespaceContext {
    return this.getRuleContext(0, PostLogicalOperatorWhitespaceContext);
  }
  constructor(ctx: ExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterIncompleteAndExpression) {
      listener.enterIncompleteAndExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitIncompleteAndExpression) {
      listener.exitIncompleteAndExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitIncompleteAndExpression) {
      return visitor.visitIncompleteAndExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class IncompleteOrExpressionContext extends ExprContext {
  public expr(): ExprContext {
    return this.getRuleContext(0, ExprContext);
  }
  public orToken(): OrTokenContext {
    return this.getRuleContext(0, OrTokenContext);
  }
  public postLogicalOperatorWhitespace(): PostLogicalOperatorWhitespaceContext {
    return this.getRuleContext(0, PostLogicalOperatorWhitespaceContext);
  }
  constructor(ctx: ExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterIncompleteOrExpression) {
      listener.enterIncompleteOrExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitIncompleteOrExpression) {
      listener.exitIncompleteOrExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitIncompleteOrExpression) {
      return visitor.visitIncompleteOrExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class IncompleteNotExpressionContext extends ExprContext {
  public notToken(): NotTokenContext {
    return this.getRuleContext(0, NotTokenContext);
  }
  public postNotOperatorWhitespace(): PostNotOperatorWhitespaceContext {
    return this.getRuleContext(0, PostNotOperatorWhitespaceContext);
  }
  constructor(ctx: ExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterIncompleteNotExpression) {
      listener.enterIncompleteNotExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitIncompleteNotExpression) {
      listener.exitIncompleteNotExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitIncompleteNotExpression) {
      return visitor.visitIncompleteNotExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class AllExpressionContext extends ExprContext {
  public STAR(): TerminalNode {
    return this.getToken(SelectionAutoCompleteParser.STAR, 0);
  }
  public postExpressionWhitespace(): PostExpressionWhitespaceContext {
    return this.getRuleContext(0, PostExpressionWhitespaceContext);
  }
  constructor(ctx: ExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterAllExpression) {
      listener.enterAllExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitAllExpression) {
      listener.exitAllExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitAllExpression) {
      return visitor.visitAllExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class UnmatchedValueContext extends ExprContext {
  public value(): ValueContext {
    return this.getRuleContext(0, ValueContext);
  }
  public postExpressionWhitespace(): PostExpressionWhitespaceContext {
    return this.getRuleContext(0, PostExpressionWhitespaceContext);
  }
  constructor(ctx: ExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterUnmatchedValue) {
      listener.enterUnmatchedValue(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitUnmatchedValue) {
      listener.exitUnmatchedValue(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitUnmatchedValue) {
      return visitor.visitUnmatchedValue(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class TraversalAllowedExprContext extends ParserRuleContext {
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_traversalAllowedExpr;
  }
  public copyFrom(ctx: TraversalAllowedExprContext): void {
    super.copyFrom(ctx);
  }
}
export class AttributeExpressionContext extends TraversalAllowedExprContext {
  public attributeName(): AttributeNameContext {
    return this.getRuleContext(0, AttributeNameContext);
  }
  public colonToken(): ColonTokenContext {
    return this.getRuleContext(0, ColonTokenContext);
  }
  public attributeValue(): AttributeValueContext {
    return this.getRuleContext(0, AttributeValueContext);
  }
  public postAttributeValueWhitespace(): PostAttributeValueWhitespaceContext {
    return this.getRuleContext(0, PostAttributeValueWhitespaceContext);
  }
  constructor(ctx: TraversalAllowedExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterAttributeExpression) {
      listener.enterAttributeExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitAttributeExpression) {
      listener.exitAttributeExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitAttributeExpression) {
      return visitor.visitAttributeExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class FunctionCallExpressionContext extends TraversalAllowedExprContext {
  public functionName(): FunctionNameContext {
    return this.getRuleContext(0, FunctionNameContext);
  }
  public parenthesizedExpr(): ParenthesizedExprContext {
    return this.getRuleContext(0, ParenthesizedExprContext);
  }
  constructor(ctx: TraversalAllowedExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterFunctionCallExpression) {
      listener.enterFunctionCallExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitFunctionCallExpression) {
      listener.exitFunctionCallExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitFunctionCallExpression) {
      return visitor.visitFunctionCallExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class TraversalAllowedParenthesizedExpressionContext extends TraversalAllowedExprContext {
  public parenthesizedExpr(): ParenthesizedExprContext {
    return this.getRuleContext(0, ParenthesizedExprContext);
  }
  constructor(ctx: TraversalAllowedExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterTraversalAllowedParenthesizedExpression) {
      listener.enterTraversalAllowedParenthesizedExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitTraversalAllowedParenthesizedExpression) {
      listener.exitTraversalAllowedParenthesizedExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitTraversalAllowedParenthesizedExpression) {
      return visitor.visitTraversalAllowedParenthesizedExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class IncompleteExpressionContext extends TraversalAllowedExprContext {
  public incompleteExpr(): IncompleteExprContext {
    return this.getRuleContext(0, IncompleteExprContext);
  }
  constructor(ctx: TraversalAllowedExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterIncompleteExpression) {
      listener.enterIncompleteExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitIncompleteExpression) {
      listener.exitIncompleteExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitIncompleteExpression) {
      return visitor.visitIncompleteExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class ParenthesizedExprContext extends ParserRuleContext {
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_parenthesizedExpr;
  }
  public copyFrom(ctx: ParenthesizedExprContext): void {
    super.copyFrom(ctx);
  }
}
export class ParenthesizedExpressionContext extends ParenthesizedExprContext {
  public leftParenToken(): LeftParenTokenContext {
    return this.getRuleContext(0, LeftParenTokenContext);
  }
  public postLogicalOperatorWhitespace(): PostLogicalOperatorWhitespaceContext {
    return this.getRuleContext(0, PostLogicalOperatorWhitespaceContext);
  }
  public expr(): ExprContext {
    return this.getRuleContext(0, ExprContext);
  }
  public rightParenToken(): RightParenTokenContext {
    return this.getRuleContext(0, RightParenTokenContext);
  }
  public postExpressionWhitespace(): PostExpressionWhitespaceContext {
    return this.getRuleContext(0, PostExpressionWhitespaceContext);
  }
  constructor(ctx: ParenthesizedExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterParenthesizedExpression) {
      listener.enterParenthesizedExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitParenthesizedExpression) {
      listener.exitParenthesizedExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitParenthesizedExpression) {
      return visitor.visitParenthesizedExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class IncompleteExprContext extends ParserRuleContext {
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_incompleteExpr;
  }
  public copyFrom(ctx: IncompleteExprContext): void {
    super.copyFrom(ctx);
  }
}
export class IncompleteAttributeExpressionMissingValueContext extends IncompleteExprContext {
  public attributeName(): AttributeNameContext {
    return this.getRuleContext(0, AttributeNameContext);
  }
  public colonToken(): ColonTokenContext {
    return this.getRuleContext(0, ColonTokenContext);
  }
  public attributeValueWhitespace(): AttributeValueWhitespaceContext {
    return this.getRuleContext(0, AttributeValueWhitespaceContext);
  }
  constructor(ctx: IncompleteExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterIncompleteAttributeExpressionMissingValue) {
      listener.enterIncompleteAttributeExpressionMissingValue(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitIncompleteAttributeExpressionMissingValue) {
      listener.exitIncompleteAttributeExpressionMissingValue(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitIncompleteAttributeExpressionMissingValue) {
      return visitor.visitIncompleteAttributeExpressionMissingValue(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class ExpressionlessFunctionExpressionContext extends IncompleteExprContext {
  public functionName(): FunctionNameContext {
    return this.getRuleContext(0, FunctionNameContext);
  }
  public expressionLessParenthesizedExpr(): ExpressionLessParenthesizedExprContext {
    return this.getRuleContext(0, ExpressionLessParenthesizedExprContext);
  }
  constructor(ctx: IncompleteExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterExpressionlessFunctionExpression) {
      listener.enterExpressionlessFunctionExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitExpressionlessFunctionExpression) {
      listener.exitExpressionlessFunctionExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitExpressionlessFunctionExpression) {
      return visitor.visitExpressionlessFunctionExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class UnclosedExpressionlessFunctionExpressionContext extends IncompleteExprContext {
  public functionName(): FunctionNameContext {
    return this.getRuleContext(0, FunctionNameContext);
  }
  public leftParenToken(): LeftParenTokenContext {
    return this.getRuleContext(0, LeftParenTokenContext);
  }
  public postLogicalOperatorWhitespace(): PostLogicalOperatorWhitespaceContext {
    return this.getRuleContext(0, PostLogicalOperatorWhitespaceContext);
  }
  constructor(ctx: IncompleteExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterUnclosedExpressionlessFunctionExpression) {
      listener.enterUnclosedExpressionlessFunctionExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitUnclosedExpressionlessFunctionExpression) {
      listener.exitUnclosedExpressionlessFunctionExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitUnclosedExpressionlessFunctionExpression) {
      return visitor.visitUnclosedExpressionlessFunctionExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class UnclosedFunctionExpressionContext extends IncompleteExprContext {
  public functionName(): FunctionNameContext {
    return this.getRuleContext(0, FunctionNameContext);
  }
  public leftParenToken(): LeftParenTokenContext {
    return this.getRuleContext(0, LeftParenTokenContext);
  }
  public expr(): ExprContext {
    return this.getRuleContext(0, ExprContext);
  }
  constructor(ctx: IncompleteExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterUnclosedFunctionExpression) {
      listener.enterUnclosedFunctionExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitUnclosedFunctionExpression) {
      listener.exitUnclosedFunctionExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitUnclosedFunctionExpression) {
      return visitor.visitUnclosedFunctionExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class UnclosedParenthesizedExpressionContext extends IncompleteExprContext {
  public leftParenToken(): LeftParenTokenContext {
    return this.getRuleContext(0, LeftParenTokenContext);
  }
  public postLogicalOperatorWhitespace(): PostLogicalOperatorWhitespaceContext {
    return this.getRuleContext(0, PostLogicalOperatorWhitespaceContext);
  }
  public expr(): ExprContext {
    return this.getRuleContext(0, ExprContext);
  }
  constructor(ctx: IncompleteExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterUnclosedParenthesizedExpression) {
      listener.enterUnclosedParenthesizedExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitUnclosedParenthesizedExpression) {
      listener.exitUnclosedParenthesizedExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitUnclosedParenthesizedExpression) {
      return visitor.visitUnclosedParenthesizedExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class ExpressionlessParenthesizedExpressionWrapperContext extends IncompleteExprContext {
  public expressionLessParenthesizedExpr(): ExpressionLessParenthesizedExprContext {
    return this.getRuleContext(0, ExpressionLessParenthesizedExprContext);
  }
  constructor(ctx: IncompleteExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterExpressionlessParenthesizedExpressionWrapper) {
      listener.enterExpressionlessParenthesizedExpressionWrapper(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitExpressionlessParenthesizedExpressionWrapper) {
      listener.exitExpressionlessParenthesizedExpressionWrapper(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitExpressionlessParenthesizedExpressionWrapper) {
      return visitor.visitExpressionlessParenthesizedExpressionWrapper(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class UnclosedExpressionlessParenthesizedExpressionContext extends IncompleteExprContext {
  public leftParenToken(): LeftParenTokenContext {
    return this.getRuleContext(0, LeftParenTokenContext);
  }
  public postLogicalOperatorWhitespace(): PostLogicalOperatorWhitespaceContext {
    return this.getRuleContext(0, PostLogicalOperatorWhitespaceContext);
  }
  constructor(ctx: IncompleteExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterUnclosedExpressionlessParenthesizedExpression) {
      listener.enterUnclosedExpressionlessParenthesizedExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitUnclosedExpressionlessParenthesizedExpression) {
      listener.exitUnclosedExpressionlessParenthesizedExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitUnclosedExpressionlessParenthesizedExpression) {
      return visitor.visitUnclosedExpressionlessParenthesizedExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class IncompletePlusTraversalExpressionContext extends IncompleteExprContext {
  public postNeighborTraversalWhitespace(): PostNeighborTraversalWhitespaceContext {
    return this.getRuleContext(0, PostNeighborTraversalWhitespaceContext);
  }
  public PLUS(): TerminalNode[];
  public PLUS(i: number): TerminalNode;
  public PLUS(i?: number): TerminalNode | TerminalNode[] {
    if (i === undefined) {
      return this.getTokens(SelectionAutoCompleteParser.PLUS);
    } else {
      return this.getToken(SelectionAutoCompleteParser.PLUS, i);
    }
  }
  constructor(ctx: IncompleteExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterIncompletePlusTraversalExpression) {
      listener.enterIncompletePlusTraversalExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitIncompletePlusTraversalExpression) {
      listener.exitIncompletePlusTraversalExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitIncompletePlusTraversalExpression) {
      return visitor.visitIncompletePlusTraversalExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class IncompleteAttributeExpressionMissingKeyContext extends IncompleteExprContext {
  public colonToken(): ColonTokenContext {
    return this.getRuleContext(0, ColonTokenContext);
  }
  public attributeValue(): AttributeValueContext {
    return this.getRuleContext(0, AttributeValueContext);
  }
  public postExpressionWhitespace(): PostExpressionWhitespaceContext {
    return this.getRuleContext(0, PostExpressionWhitespaceContext);
  }
  constructor(ctx: IncompleteExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterIncompleteAttributeExpressionMissingKey) {
      listener.enterIncompleteAttributeExpressionMissingKey(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitIncompleteAttributeExpressionMissingKey) {
      listener.exitIncompleteAttributeExpressionMissingKey(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitIncompleteAttributeExpressionMissingKey) {
      return visitor.visitIncompleteAttributeExpressionMissingKey(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class ExpressionLessParenthesizedExprContext extends ParserRuleContext {
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_expressionLessParenthesizedExpr;
  }
  public copyFrom(ctx: ExpressionLessParenthesizedExprContext): void {
    super.copyFrom(ctx);
  }
}
export class ExpressionlessParenthesizedExpressionContext extends ExpressionLessParenthesizedExprContext {
  public leftParenToken(): LeftParenTokenContext {
    return this.getRuleContext(0, LeftParenTokenContext);
  }
  public postLogicalOperatorWhitespace(): PostLogicalOperatorWhitespaceContext {
    return this.getRuleContext(0, PostLogicalOperatorWhitespaceContext);
  }
  public rightParenToken(): RightParenTokenContext {
    return this.getRuleContext(0, RightParenTokenContext);
  }
  public postExpressionWhitespace(): PostExpressionWhitespaceContext {
    return this.getRuleContext(0, PostExpressionWhitespaceContext);
  }
  constructor(ctx: ExpressionLessParenthesizedExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterExpressionlessParenthesizedExpression) {
      listener.enterExpressionlessParenthesizedExpression(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitExpressionlessParenthesizedExpression) {
      listener.exitExpressionlessParenthesizedExpression(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitExpressionlessParenthesizedExpression) {
      return visitor.visitExpressionlessParenthesizedExpression(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class UpTraversalExprContext extends ParserRuleContext {
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_upTraversalExpr;
  }
  public copyFrom(ctx: UpTraversalExprContext): void {
    super.copyFrom(ctx);
  }
}
export class UpTraversalContext extends UpTraversalExprContext {
  public traversal(): TraversalContext {
    return this.getRuleContext(0, TraversalContext);
  }
  public postUpwardTraversalWhitespace(): PostUpwardTraversalWhitespaceContext {
    return this.getRuleContext(0, PostUpwardTraversalWhitespaceContext);
  }
  constructor(ctx: UpTraversalExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterUpTraversal) {
      listener.enterUpTraversal(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitUpTraversal) {
      listener.exitUpTraversal(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitUpTraversal) {
      return visitor.visitUpTraversal(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class DownTraversalExprContext extends ParserRuleContext {
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_downTraversalExpr;
  }
  public copyFrom(ctx: DownTraversalExprContext): void {
    super.copyFrom(ctx);
  }
}
export class DownTraversalContext extends DownTraversalExprContext {
  public traversal(): TraversalContext {
    return this.getRuleContext(0, TraversalContext);
  }
  public postDownwardTraversalWhitespace(): PostDownwardTraversalWhitespaceContext {
    return this.getRuleContext(0, PostDownwardTraversalWhitespaceContext);
  }
  constructor(ctx: DownTraversalExprContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterDownTraversal) {
      listener.enterDownTraversal(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitDownTraversal) {
      listener.exitDownTraversal(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitDownTraversal) {
      return visitor.visitDownTraversal(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class TraversalContext extends ParserRuleContext {
  public STAR(): TerminalNode | undefined {
    return this.tryGetToken(SelectionAutoCompleteParser.STAR, 0);
  }
  public PLUS(): TerminalNode[];
  public PLUS(i: number): TerminalNode;
  public PLUS(i?: number): TerminalNode | TerminalNode[] {
    if (i === undefined) {
      return this.getTokens(SelectionAutoCompleteParser.PLUS);
    } else {
      return this.getToken(SelectionAutoCompleteParser.PLUS, i);
    }
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_traversal;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterTraversal) {
      listener.enterTraversal(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitTraversal) {
      listener.exitTraversal(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitTraversal) {
      return visitor.visitTraversal(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class AttributeNameContext extends ParserRuleContext {
  public IDENTIFIER(): TerminalNode {
    return this.getToken(SelectionAutoCompleteParser.IDENTIFIER, 0);
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_attributeName;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterAttributeName) {
      listener.enterAttributeName(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitAttributeName) {
      listener.exitAttributeName(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitAttributeName) {
      return visitor.visitAttributeName(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class AttributeValueContext extends ParserRuleContext {
  public value(): ValueContext {
    return this.getRuleContext(0, ValueContext);
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_attributeValue;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterAttributeValue) {
      listener.enterAttributeValue(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitAttributeValue) {
      listener.exitAttributeValue(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitAttributeValue) {
      return visitor.visitAttributeValue(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class FunctionNameContext extends ParserRuleContext {
  public IDENTIFIER(): TerminalNode {
    return this.getToken(SelectionAutoCompleteParser.IDENTIFIER, 0);
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_functionName;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterFunctionName) {
      listener.enterFunctionName(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitFunctionName) {
      listener.exitFunctionName(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitFunctionName) {
      return visitor.visitFunctionName(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class OrTokenContext extends ParserRuleContext {
  public OR(): TerminalNode {
    return this.getToken(SelectionAutoCompleteParser.OR, 0);
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_orToken;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterOrToken) {
      listener.enterOrToken(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitOrToken) {
      listener.exitOrToken(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitOrToken) {
      return visitor.visitOrToken(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class AndTokenContext extends ParserRuleContext {
  public AND(): TerminalNode {
    return this.getToken(SelectionAutoCompleteParser.AND, 0);
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_andToken;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterAndToken) {
      listener.enterAndToken(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitAndToken) {
      listener.exitAndToken(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitAndToken) {
      return visitor.visitAndToken(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class NotTokenContext extends ParserRuleContext {
  public NOT(): TerminalNode {
    return this.getToken(SelectionAutoCompleteParser.NOT, 0);
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_notToken;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterNotToken) {
      listener.enterNotToken(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitNotToken) {
      listener.exitNotToken(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitNotToken) {
      return visitor.visitNotToken(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class ColonTokenContext extends ParserRuleContext {
  public COLON(): TerminalNode {
    return this.getToken(SelectionAutoCompleteParser.COLON, 0);
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_colonToken;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterColonToken) {
      listener.enterColonToken(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitColonToken) {
      listener.exitColonToken(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitColonToken) {
      return visitor.visitColonToken(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class LeftParenTokenContext extends ParserRuleContext {
  public LPAREN(): TerminalNode {
    return this.getToken(SelectionAutoCompleteParser.LPAREN, 0);
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_leftParenToken;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterLeftParenToken) {
      listener.enterLeftParenToken(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitLeftParenToken) {
      listener.exitLeftParenToken(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitLeftParenToken) {
      return visitor.visitLeftParenToken(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class RightParenTokenContext extends ParserRuleContext {
  public RPAREN(): TerminalNode {
    return this.getToken(SelectionAutoCompleteParser.RPAREN, 0);
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_rightParenToken;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterRightParenToken) {
      listener.enterRightParenToken(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitRightParenToken) {
      listener.exitRightParenToken(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitRightParenToken) {
      return visitor.visitRightParenToken(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class AttributeValueWhitespaceContext extends ParserRuleContext {
  public WS(): TerminalNode[];
  public WS(i: number): TerminalNode;
  public WS(i?: number): TerminalNode | TerminalNode[] {
    if (i === undefined) {
      return this.getTokens(SelectionAutoCompleteParser.WS);
    } else {
      return this.getToken(SelectionAutoCompleteParser.WS, i);
    }
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_attributeValueWhitespace;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterAttributeValueWhitespace) {
      listener.enterAttributeValueWhitespace(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitAttributeValueWhitespace) {
      listener.exitAttributeValueWhitespace(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitAttributeValueWhitespace) {
      return visitor.visitAttributeValueWhitespace(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class PostAttributeValueWhitespaceContext extends ParserRuleContext {
  public WS(): TerminalNode[];
  public WS(i: number): TerminalNode;
  public WS(i?: number): TerminalNode | TerminalNode[] {
    if (i === undefined) {
      return this.getTokens(SelectionAutoCompleteParser.WS);
    } else {
      return this.getToken(SelectionAutoCompleteParser.WS, i);
    }
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_postAttributeValueWhitespace;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterPostAttributeValueWhitespace) {
      listener.enterPostAttributeValueWhitespace(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitPostAttributeValueWhitespace) {
      listener.exitPostAttributeValueWhitespace(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitPostAttributeValueWhitespace) {
      return visitor.visitPostAttributeValueWhitespace(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class PostExpressionWhitespaceContext extends ParserRuleContext {
  public WS(): TerminalNode[];
  public WS(i: number): TerminalNode;
  public WS(i?: number): TerminalNode | TerminalNode[] {
    if (i === undefined) {
      return this.getTokens(SelectionAutoCompleteParser.WS);
    } else {
      return this.getToken(SelectionAutoCompleteParser.WS, i);
    }
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_postExpressionWhitespace;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterPostExpressionWhitespace) {
      listener.enterPostExpressionWhitespace(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitPostExpressionWhitespace) {
      listener.exitPostExpressionWhitespace(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitPostExpressionWhitespace) {
      return visitor.visitPostExpressionWhitespace(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class PostNotOperatorWhitespaceContext extends ParserRuleContext {
  public WS(): TerminalNode[];
  public WS(i: number): TerminalNode;
  public WS(i?: number): TerminalNode | TerminalNode[] {
    if (i === undefined) {
      return this.getTokens(SelectionAutoCompleteParser.WS);
    } else {
      return this.getToken(SelectionAutoCompleteParser.WS, i);
    }
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_postNotOperatorWhitespace;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterPostNotOperatorWhitespace) {
      listener.enterPostNotOperatorWhitespace(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitPostNotOperatorWhitespace) {
      listener.exitPostNotOperatorWhitespace(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitPostNotOperatorWhitespace) {
      return visitor.visitPostNotOperatorWhitespace(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class PostLogicalOperatorWhitespaceContext extends ParserRuleContext {
  public WS(): TerminalNode[];
  public WS(i: number): TerminalNode;
  public WS(i?: number): TerminalNode | TerminalNode[] {
    if (i === undefined) {
      return this.getTokens(SelectionAutoCompleteParser.WS);
    } else {
      return this.getToken(SelectionAutoCompleteParser.WS, i);
    }
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_postLogicalOperatorWhitespace;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterPostLogicalOperatorWhitespace) {
      listener.enterPostLogicalOperatorWhitespace(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitPostLogicalOperatorWhitespace) {
      listener.exitPostLogicalOperatorWhitespace(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitPostLogicalOperatorWhitespace) {
      return visitor.visitPostLogicalOperatorWhitespace(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class PostNeighborTraversalWhitespaceContext extends ParserRuleContext {
  public WS(): TerminalNode[];
  public WS(i: number): TerminalNode;
  public WS(i?: number): TerminalNode | TerminalNode[] {
    if (i === undefined) {
      return this.getTokens(SelectionAutoCompleteParser.WS);
    } else {
      return this.getToken(SelectionAutoCompleteParser.WS, i);
    }
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_postNeighborTraversalWhitespace;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterPostNeighborTraversalWhitespace) {
      listener.enterPostNeighborTraversalWhitespace(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitPostNeighborTraversalWhitespace) {
      listener.exitPostNeighborTraversalWhitespace(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitPostNeighborTraversalWhitespace) {
      return visitor.visitPostNeighborTraversalWhitespace(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class PostUpwardTraversalWhitespaceContext extends ParserRuleContext {
  public WS(): TerminalNode[];
  public WS(i: number): TerminalNode;
  public WS(i?: number): TerminalNode | TerminalNode[] {
    if (i === undefined) {
      return this.getTokens(SelectionAutoCompleteParser.WS);
    } else {
      return this.getToken(SelectionAutoCompleteParser.WS, i);
    }
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_postUpwardTraversalWhitespace;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterPostUpwardTraversalWhitespace) {
      listener.enterPostUpwardTraversalWhitespace(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitPostUpwardTraversalWhitespace) {
      listener.exitPostUpwardTraversalWhitespace(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitPostUpwardTraversalWhitespace) {
      return visitor.visitPostUpwardTraversalWhitespace(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class PostDownwardTraversalWhitespaceContext extends ParserRuleContext {
  public WS(): TerminalNode[];
  public WS(i: number): TerminalNode;
  public WS(i?: number): TerminalNode | TerminalNode[] {
    if (i === undefined) {
      return this.getTokens(SelectionAutoCompleteParser.WS);
    } else {
      return this.getToken(SelectionAutoCompleteParser.WS, i);
    }
  }
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_postDownwardTraversalWhitespace;
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterPostDownwardTraversalWhitespace) {
      listener.enterPostDownwardTraversalWhitespace(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitPostDownwardTraversalWhitespace) {
      listener.exitPostDownwardTraversalWhitespace(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitPostDownwardTraversalWhitespace) {
      return visitor.visitPostDownwardTraversalWhitespace(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}

export class ValueContext extends ParserRuleContext {
  constructor(parent: ParserRuleContext | undefined, invokingState: number) {
    super(parent, invokingState);
  }
  // @Override
  public get ruleIndex(): number {
    return SelectionAutoCompleteParser.RULE_value;
  }
  public copyFrom(ctx: ValueContext): void {
    super.copyFrom(ctx);
  }
}
export class QuotedStringValueContext extends ValueContext {
  public QUOTED_STRING(): TerminalNode {
    return this.getToken(SelectionAutoCompleteParser.QUOTED_STRING, 0);
  }
  constructor(ctx: ValueContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterQuotedStringValue) {
      listener.enterQuotedStringValue(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitQuotedStringValue) {
      listener.exitQuotedStringValue(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitQuotedStringValue) {
      return visitor.visitQuotedStringValue(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class IncompleteLeftQuotedStringValueContext extends ValueContext {
  public INCOMPLETE_LEFT_QUOTED_STRING(): TerminalNode {
    return this.getToken(SelectionAutoCompleteParser.INCOMPLETE_LEFT_QUOTED_STRING, 0);
  }
  constructor(ctx: ValueContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterIncompleteLeftQuotedStringValue) {
      listener.enterIncompleteLeftQuotedStringValue(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitIncompleteLeftQuotedStringValue) {
      listener.exitIncompleteLeftQuotedStringValue(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitIncompleteLeftQuotedStringValue) {
      return visitor.visitIncompleteLeftQuotedStringValue(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class IncompleteRightQuotedStringValueContext extends ValueContext {
  public INCOMPLETE_RIGHT_QUOTED_STRING(): TerminalNode {
    return this.getToken(SelectionAutoCompleteParser.INCOMPLETE_RIGHT_QUOTED_STRING, 0);
  }
  constructor(ctx: ValueContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterIncompleteRightQuotedStringValue) {
      listener.enterIncompleteRightQuotedStringValue(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitIncompleteRightQuotedStringValue) {
      listener.exitIncompleteRightQuotedStringValue(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitIncompleteRightQuotedStringValue) {
      return visitor.visitIncompleteRightQuotedStringValue(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
export class UnquotedStringValueContext extends ValueContext {
  public IDENTIFIER(): TerminalNode {
    return this.getToken(SelectionAutoCompleteParser.IDENTIFIER, 0);
  }
  constructor(ctx: ValueContext) {
    super(ctx.parent, ctx.invokingState);
    this.copyFrom(ctx);
  }
  // @Override
  public enterRule(listener: SelectionAutoCompleteListener): void {
    if (listener.enterUnquotedStringValue) {
      listener.enterUnquotedStringValue(this);
    }
  }
  // @Override
  public exitRule(listener: SelectionAutoCompleteListener): void {
    if (listener.exitUnquotedStringValue) {
      listener.exitUnquotedStringValue(this);
    }
  }
  // @Override
  public accept<Result>(visitor: SelectionAutoCompleteVisitor<Result>): Result {
    if (visitor.visitUnquotedStringValue) {
      return visitor.visitUnquotedStringValue(this);
    } else {
      return visitor.visitChildren(this);
    }
  }
}
