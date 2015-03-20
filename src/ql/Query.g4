grammar Query;

/***************
 * PARSER RULES
 ***************/

// START SYMBOL.
filter: junctive_expression EOF ;

junctive_expression
    : junctive_expression AND set_expression # andExpr
    | junctive_expression OR set_expression  # orExpr
    | set_expression                         # setExpr
    ;

set_expression
    : set_expression IN set_literal # inExpr
    | comparative_expression        # cmpExpr
    ;

comparative_expression
    : comparative_expression EQ negative_expression # eqExpr
    | comparative_expression LIKE string_literal    # likeExpr
    | comparative_expression REGEX string_literal   # regexExpr
    | negative_expression                           # negExpr
    ;

negative_expression
    : NOT negative_expression # notExpr
    | atomic_expression       # atomExpr
    ;

atomic_expression
    : LPAREN junctive_expression RPAREN
    | identifier
    | atomic_literal
    ;

identifier: ID ;

// LITERALS.
atomic_literal
    : boolean_literal
    | string_literal
    | numeric_literal
    ;

boolean_literal
    : TRUE
    | FALSE
    ;

string_literal: STR ;

numeric_literal: INT ;

set_literal: LPAREN atomic_literal (COMMA atomic_literal)* RPAREN ;

/**************
 * LEXER RULES
 **************/

// WHITESPACE.
WS: [ \t\r\n]+ -> skip ;

// SIMPLE TOKENS.
TRUE: 'true' ;
FALSE: 'false' ;
AND: 'and' ;
OR: 'or' ;
IN: 'in' ;
EQ: '=' ;
LIKE: 'like' ;
REGEX: '~' ;
NOT: 'not' ;
LPAREN: '(' ;
RPAREN: ')' ;
COMMA: ',' ;
QUOTE: '\'' ;
DQUOTE: '"' ;
MINUS: '-';

// COMPLEX TOKENS.
fragment NONZERO: [1-9] ;
fragment ZERO: '0' ;
fragment DIGIT
    : ZERO
    | NONZERO
    ;

fragment LETTER: [a-zA-Z] ;

fragment USCORE: '_' ;

ID: (LETTER | USCORE) (LETTER | USCORE | DIGIT)* ;

INT
    : MINUS? NONZERO DIGIT* 
    | ZERO
    ;

fragment STRCHAR: [a-zA-Z0-9_\\.\- *] ;

STR
    : QUOTE STRCHAR* QUOTE
    | DQUOTE STRCHAR* DQUOTE
    ;

// Ensure everything not yet matched gets lexed.
ErrorCharacter: . ;

