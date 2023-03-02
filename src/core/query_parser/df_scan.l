%top{
// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

}

%{

// The code here is generated in cc file.

%}

%option yylineno noyywrap reentrant
%option nodefault

/*
 * Set the type of yyextra.  All state variables used by the scanner should
 * be in yyextra, *not* statically allocated.
*/
%option extra-type="int"

/* remove unused functions*/
%option noinput nounput

%option warn
%option prefix="df"
%option bison-bridge bison-locations

%%
 . { }

%%
