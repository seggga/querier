package main

import (
	"errors"

	"github.com/seggga/querier/internal/pkg/mylexer"
	"github.com/seggga/querier/internal/pkg/myscanner"
	"github.com/seggga/querier/internal/pkg/mytoken"
)

func fillLexMachine(query string) (*mylexer.LexMachine, error) {

	// check user's query
	if !mylexer.CheckQueryPattern(query) {
		err := errors.New("wrong query")
		return nil, err
	}

	// scanner initialisation
	var scanner myscanner.Scanner
	fset := mytoken.NewFileSet()
	file := fset.AddFile("", fset.Base(), len(query))
	scanner.Init(file, []byte(query), nil, myscanner.ScanComments)

	var lm mylexer.LexMachine
	lm.Query = string(query)

	// run the scanner, initiate LexMachine
	for {
		_, tok, lit := scanner.Scan()
		if tok == mytoken.EOF {
			break
		}
		mylexer.AnalyseToken(&lm, lit, tok)
		//fmt.Printf("%s\t%s\t%q\n", fset.Position(pos), tok, lit)
	}

	// check if the query contains at least one file to be read
	if len(lm.From) == 0 {
		err := errors.New("no file has been chosen (section FROM is empty)")
		return nil, err
	}

	// check if the query contains at least one column to be written to output
	if len(lm.Select) == 0 {
		err := errors.New("no columns has been chosen (section SELECT is empty)")
		return nil, err
	}

	return &lm, nil

}
