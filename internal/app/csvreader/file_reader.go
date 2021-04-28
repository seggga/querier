package csvreader

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/seggga/querier/internal/pkg/mylexer"
)

func ReadTheFile(lm *mylexer.LexMachine, ctx context.Context, finishChan chan struct{}) error {

	defer func() {
		finishChan <- struct{}{}
	}()

	// read files
	// открытие файлов
	//		 прочитали заголовок CSV
	//		 проверка, все ли столбцы в блоке select есть в таблице из файла
	//		 проверка, все ли столбцы в блоке where есть в таблице из файла
	//		 считываем строки
	//		 		подставляем данные из таблицы в слайс вычисления
	//				вывод, если строка соответствует условию
	for _, fileName := range lm.From {

		if _, err := os.Stat(fileName); err != nil {
			return fmt.Errorf("file %s was not found. %w", fileName, err)
		}

		// file opening
		file, err := os.OpenFile(fileName, os.O_RDONLY, 0600)
		if err != nil {
			return fmt.Errorf("unable to read file %s. %w", fileName, err)
		}

		// deffered file closing
		defer func(filename string, file *os.File) {
			// closing file
			if err := file.Close(); err != nil {
				log.Fatalf("error while closing a file %s %v", fileName, err)
			}
		}(fileName, file)

		// read the header of the csv-file
		reader := csv.NewReader(file)  // Считываем файл с помощью библиотеки encoding/csv
		fileCols, err := reader.Read() //  Считываем шапку таблицы
		if err != nil {
			log.Fatalf("Cannot read file %s: %v", fileName, err)
		}

		// compare columns sets from the query and the file
		err = mylexer.CheckSelectedColumns(fileCols, lm)
		if err != nil {
			log.Fatalf("Неверный запрос: %v", err)
		}

		for {

			select {
			case <-ctx.Done():
				return nil
			default:

				row, err := reader.Read()
				if err == io.EOF {
					break
				}

				if err != nil {
					log.Fatalf("Error reading csv-file %s: %v", fileName, err)
				}
				// compose a map holding data of the current row
				rowData := mylexer.FillTheMap(fileCols, row, lm)
				// create a slice based on the conditions in WHERE-statement
				lexSlice := mylexer.MakeSlice(rowData, lm)

				if mylexer.Execute(lexSlice) {
					_ = mylexer.PrintTheRow(rowData, lm)
				}
			}
		}

	}
	return nil
}
