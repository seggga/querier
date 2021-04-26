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

func CheckSelectedColumns(s []string, lm LexMachine) error {
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

	// check WHERE statement
	counter = len(lm.Where)
	for i := 0; i < len(lm.Where); i += 4 { //подход не учитывает скобки. Надо исправлять
		for _, colInTable := range s {
			if lm.Where[i].Tok == colInTable {
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
func CheckQueryPattern(b []byte) bool {

	theQuery := string(b)
	theQuery = strings.ToLower(theQuery)
	theQuery = strings.TrimSpace(theQuery)

	// проверка на первый key_word
	if !strings.HasPrefix(theQuery, "select") {
		return false
	}

	for _, patt := range QueryPatterns {
		matched, _ := regexp.Match(patt, []byte(theQuery))
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
			fmt.Println(sl)
			if res.Tok == "true" {
				return true
			}
			return false
		}
		// нефинальное вычисление
		sl = append(sl, calculator(sl[i:i+3]))
		if sl[i+3].Lex == "operator" {
			sl = append(sl, sl[i+3])
		} else {
			i -= 1
		}
	}
	return false
}

func calculator(ops []Lexemma) Lexemma {

	for i, op := range ops {
		if op.Lex == "operator" {
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
		result = operand1.Tok > operand2.Tok
	case ">=":
		result = operand1.Tok >= operand2.Tok
	case "==":
		result = operand1.Tok == operand2.Tok
	case "<":
		result = operand1.Tok < operand2.Tok
	case "<=":
		result = operand1.Tok <= operand2.Tok
	case "and":
		result = (operand1.Tok == "true") && (operand2.Tok == "true")
	case "or":
		result = (operand1.Tok == "true") || (operand2.Tok == "true")
	}

	if result {
		return Lexemma{"bool", "true"}
	}
	return Lexemma{"bool", "false"}
}

// FillTheMap constructs the map for mapping column-name => current value from the given row of csv-file
func FillTheMap(fileCols, row []string, lm LexMachine) map[string]string {

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
	for i := 0; i < len(lm.Where); i += 4 { //подход не учитывает скобки. Надо исправлять
		for j := 0; j < len(fileCols); j += 1 {
			if lm.Where[i].Tok == fileCols[j] {
				rowData[lm.Where[i].Tok] = row[j]
			}
		}
	}

	return rowData
}

func MakeSlice(rowData map[string]string, lm LexMachine) []Lexemma {

	lexSlice := make([]Lexemma, len(lm.Where))

	for _, lex := range lm.Where {
		if lex.Lex == "IDENT" {
			lex.Tok = rowData[lex.Tok]
			lexSlice = append(lexSlice, lex)
		}
	}
	return nil
}

func PrintTheRow(rowData map[string]string, lm LexMachine) error {

	for _, fieldName := range lm.Select {
		fmt.Printf("%s\t", rowData[fieldName])
	}
	fmt.Println()
	return nil
}

/*
 slice += execute(slice[i], slice[i+1], slice[i+2])  // внутри функции идет проверка ситуации, есть ли четвертый элемент в группе.
 //если четвертого в группе нет, значит это финальное вычисление
 if slice[i+3] isOperator {
	 slice += slice[i+3]
 } else { // закончились операторы второго уровня
	i--
 }
	return false
}

/*   age >= 30 AND region=="Europe" OR status == "sick"

1) сформировать слайс операндов и действий с ними и выполнять по 3


age, >=, 30, AND, | region, ==, europe, AND, | status, ==, sick, | true, AND, false, AND, | true | false AND  >= false

===================================

/*   age >= 30 AND region=="Europe"

age, >=, 30, AND, | region, ==, europe | true, AND, false, | false

====================================

for i := 0; i < len(operators); i += 4

 slice += execute(slice[i], slice[i+1], slice[i+2])  // внутри функции идет проверка ситуации, есть ли четвертый элемент в группе.
 //если четвертого в группе нет, значит это финальное вычисление
 if slice[i+3] isOperator {
	 slice += slice[i+3]
 } else { // закончились операторы второго уровня
	i--
 }

if slice[len(slice)] -> print the string

/*


1) age >= 30 region == "europe" status == "sick"
2) result1 AND result2 AND result3
в перенос AND на позицию 7 - еще перенос на позицию +4 = 11
второй перенос AND на позицию

	lexerOut := []struct{
		lexType columnName / condition / value / operator /
		lexText string
		lexPriority int //
		resultPriority int // для condition

				columnName - 0
				condition - 1
				value - 0
				operator - 2

col - age - 0
cond - >= - 1
val - 30 - 0


for trippleToken = range trippleTokens {


}

1)
func exec (token1, condition, token3 ) token {
	if condition == ">=" {
		return token1 >= token3
	}

	if condition == "==" {
		return token1 == token3
	}

	if condition == "AND" {
		return token AND token
	}
}


call[0]
call[1]
call[2]


equation = GOE(col, val)



	}



	return nil
}
*/
