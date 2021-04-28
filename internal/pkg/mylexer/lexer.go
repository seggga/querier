package mylexer

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/seggga/querier/internal/pkg/mytoken"
)

type Lexemma struct {
	Lex string
	Tok string
}

func (l *Lexemma) isOperator() bool {
	switch {
	case l.Tok == "and", l.Tok == "or":
		return true
	case l.Tok == ">=", l.Tok == "<=", l.Tok == "==", l.Tok == ">", l.Tok == "<":
		return true
	}
	return false
}

type LexMachine struct {
	state  int
	Select []string  // набор выбранных столбцов в пользовательском запросе (блок SELECT)
	From   []string  // набор выбранных файлов, из которых будут забираться данные (блок FROM)
	Where  []Lexemma // набор условий в запросе - блок WHERE
	Query  string
	//whereStatement []string // a part of the query containing only WHERE-statement
}

// AnalyseToken - распределяет токены по слайсам, соответствующим блокам SELECT FROM и WHERE
func AnalyseToken(l *LexMachine, s string, tok mytoken.Token) {

	// исключаем запятую из лексемм
	if s == "" && tok.String() == "," {
		return
	}

	switch s {
	case "select":
		l.state = 1
		return
	case "from":
		l.state = 2
		return
	case "where":
		l.state = 3
		return
	}

	// обрезаем лишние кавычки
	if tok.String() == "STRING" {
		s = s[1 : len(s)-1]
	}

	switch l.state {
	case 1:
		l.Select = append(l.Select, s)
	case 2:
		l.From = append(l.From, s)
	case 3:
		l.Where = append(l.Where, Lexemma{s, tok.String()})
		/*		if isComparator(tok) {
					l.Condition[len(l.Condition) - 2].Typ = "column"
				}
		*/
	}
}

// функция проверяет, содержатся ли указанные в select столбцы в переданном файле
// input parameters are:
//		slice of conumns names, obtainet from the *.csv file
//		user's query
// output: a slice of column names that where found in the query
func TrimOutput(allColumns []string, b []byte) []string {

	theQuery := string(b)
	theQuery = strings.ToLower(theQuery)
	theQuery = strings.TrimSpace(theQuery)

	theQuery = strings.TrimLeft(theQuery, "select")
	theQuery = strings.Split(theQuery, "from")[0]

	theQuery = strings.TrimSpace(theQuery)
	outColumns := strings.Split(theQuery, ",")

	counter := len(outColumns)
	for _, colInQuery := range outColumns {
		for _, colInTable := range allColumns {
			if colInQuery == colInTable {
				counter--
				break
			}
		}
	}

	if counter > 0 {
		return nil // неверный набор столбцов в запросе. Имеются столбцы...allColumns
	}

	return outColumns
}

func CheckSelectedColumns(s []string, lm *LexMachine) error {
	// check SELECT statement
	counter := len(lm.Select)
	for _, colInQuery := range lm.Select {
		for _, colInTable := range s {
			if colInQuery == colInTable {
				counter--
				break
			}
		}
	}
	if counter > 0 {
		return errors.New("Набор столбцов в запросе (секция SELECT) не соответствует именам столбцов в файле")
	}

	// extract colunms frome WHERE statement
	var colsInWhere []string
	for _, colInQuery := range lm.Where {
		if colInQuery.Tok == "IDENT" {
			colsInWhere = append(colsInWhere, colInQuery.Lex)
		}
	}

	// check WHERE statement
	counter = len(colsInWhere)
	for _, colInQuery := range colsInWhere {
		for _, colInTable := range s {
			if colInQuery == colInTable {
				counter--
				break
			}
		}
	}

	if counter > 0 {
		return errors.New("Набор столбцов в запросе (секция WHERE) не соответствует именам столбцов в файле")
	}

	return nil

}

// CheckQueryPattern - checks the query pattern
// if there is no matcing pattern, the query is incorrect
func CheckQueryPattern(query string) bool {

	query = strings.ToLower(query)
	query = strings.TrimSpace(query)

	// проверка на первый key_word
	if !strings.HasPrefix(query, "select") {
		return false
	}

	for _, patt := range QueryPatterns {
		matched, _ := regexp.Match(patt, []byte(query))
		if matched {
			return true // также надо запомнить, какой паттерн подошел
		}
	}
	return false
}

func GetConditions(b []byte) []string {

	// obtain the substring that contains conditions only
	theQuery := string(b)
	theQuery = strings.ToLower(theQuery)
	theQuery = strings.TrimSpace(theQuery)

	theQuery = strings.Split(theQuery, "where")[1]

	return nil
}

func Execute(sl []Lexemma) bool {
	for i := 0; i < len(sl); i += 4 {
		// финальное вычисление ??
		if i+3 >= len(sl) {
			res := calculator(sl[i : i+3])
			return res.Lex == "true"
		}
		// нефинальное вычисление
		sl = append(sl, calculator(sl[i:i+3]))
		if sl[i+3].isOperator() {
			sl = append(sl, sl[i+3])
		} else {
			i -= 1
		}
	}
	return false
}

func calculator(ops []Lexemma) Lexemma {

	for i, op := range ops {
		if op.isOperator() {
			return calculate(i, ops)
		}
	}

	return Lexemma{}
}

func calculate(i int, ops []Lexemma) Lexemma {

	var operand1, operand2 Lexemma
	switch i {
	case 0:
		operand1 = ops[1]
		operand2 = ops[2]
	case 1:
		operand1 = ops[0]
		operand2 = ops[2]
	case 2:
		operand1 = ops[0]
		operand2 = ops[1]
	}

	var result bool
	switch ops[i].Tok {
	case ">":
		result = operand1.Lex > operand2.Lex
	case ">=":
		result = operand1.Lex >= operand2.Lex
	case "==":
		result = operand1.Lex == operand2.Lex
	case "<":
		result = operand1.Lex < operand2.Lex
	case "<=":
		result = operand1.Lex <= operand2.Lex
	case "and":
		result = (operand1.Lex == "true") && (operand2.Lex == "true")
	case "or":
		result = (operand1.Lex == "true") || (operand2.Lex == "true")
	}

	if result {
		return Lexemma{Lex: "true", Tok: "bool"}
	}
	return Lexemma{Lex: "false", Tok: "bool"}
}

// FillTheMap constructs the map for mapping column-name => current value from the given row of csv-file
func FillTheMap(fileCols, row []string, lm *LexMachine) map[string]string {

	// the output map will contain pairs 'fieldName' -> 'fieldValue' from the current row of CSV-file
	mapSize := len(lm.Select) + len(lm.Where)/3
	rowData := make(map[string]string, mapSize)

	// fill the output map with SELECT-data
	for _, col := range lm.Select {
		for i := 0; i < len(fileCols); i += 1 {
			if col == fileCols[i] {
				rowData[col] = row[i]
			}
		}
	}
	// add WHERE-data to the output map
	for _, lexemma := range lm.Where { //подход не учитывает скобки. Надо исправлять
		if lexemma.Tok != "IDENT" {
			continue
		}
		for j := 0; j < len(fileCols); j += 1 {
			if lexemma.Lex == fileCols[j] {
				rowData[lexemma.Lex] = row[j]
			}
		}
	}
	return rowData
}

func MakeSlice(rowData map[string]string, lm *LexMachine) []Lexemma {

	var lexSlice []Lexemma

	for _, lexemma := range lm.Where {
		switch lexemma.Tok {
		case "IDENT":
			lexemma.Lex = rowData[lexemma.Lex] // change the Lex field of current lexemma (column's name) with it's actual value (from rowData)
		case ";":
			continue // final symbol EOF - no need to append
		}
		lexSlice = append(lexSlice, lexemma)
	}

	return lexSlice
}

func PrintTheRow(rowData map[string]string, lm *LexMachine) error {

	for _, fieldName := range lm.Select {
		fmt.Printf("%s\t", rowData[fieldName])
	}
	fmt.Println()
	return nil
}
