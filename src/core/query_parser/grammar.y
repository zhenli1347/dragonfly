// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//


%skeleton "lalr1.cc" /* -*- C++ -*- */
%require "3.0.2"
%defines
%define api.parser.class { dfly_parser }
%define api.namespace { ::dfly }
%define api.prefix {df}

%define api.value.type {union ScanValue { int i; double d;} }
/*
to remove union and use clean semantic types
%define api.token.constructor
variant
*/

%define parse.assert

%expect 0

%locations
%initial-action
{
  // Initialize the initial location.

};

%define parse.trace
%define parse.error verbose
%define api.token.prefix {TOK_}

%parse-param { yyscan_t scanner_ }
%parse-param { void* handler_ }
%lex-param   { yyscan_t scanner_ }


// The following code goes into header file.
%code requires {

namespace dfly {
}  // dfly

#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void *yyscan_t;
#endif

}

// This code goes into cc file
%code {
#include <stdio.h>

}  // End of cc code block.

%token END  0  "end of file"

%% /* Grammar rules and actions follow */

input:    /* empty */
        | input line
;

line:     '\n'
        | END '\n'  { }
;

%code provides {
// #define YYSTYPE
// #define YYLTYPE
};

%%
