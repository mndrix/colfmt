package colfmt // import "github.com/mndrix/colfmt"
import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func Main() {
	var inputRecordSeparator byte = '\n'
	var inputFieldSeparator byte = '\t'
	outputRecordSeparator := "\n"
	outputFieldSeparator := "  "

	// collect rows
	var rows [][]string
	s := bufio.NewScanner(os.Stdin)
	s.Split(on(inputRecordSeparator))
	for s.Scan() {
		line := s.Bytes()
		columns := bytes.Split(line, []byte{inputFieldSeparator})
		strs := make([]string, len(columns))
		for i, column := range columns {
			strs[i] = string(column) // copy, since scanner reuses byte array
		}
		rows = append(rows, strs)
	}
	if err := s.Err(); err != nil {
		die("reading line: %s", err)
	}
	if len(rows) == 0 {
		return
	}

	// calculate column widths
	widths := make([]int, len(rows[0]))
	for _, row := range rows {
		if len(row) != len(widths) {
			die("Not all records have the same number of fields")
		}
		for j, column := range row {
			if len(column) > widths[j] {
				widths[j] = len(column)
			}
		}
	}

	// create format strings
	formats := make([]string, len(widths))
	for i, width := range widths {
		formats[i] = "%-" + strconv.Itoa(width) + "s"
	}

	// output formatted data
	columns := make([]string, len(widths))
	for _, row := range rows {
		for i, format := range formats {
			columns[i] = fmt.Sprintf(format, row[i])
		}
		line := strings.Join(columns, outputFieldSeparator)
		io.WriteString(os.Stdout, line)
		io.WriteString(os.Stdout, outputRecordSeparator)
	}
}

func die(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
	os.Exit(1)
}

func on(delimiter byte) bufio.SplitFunc {
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF {
			if len(data) == 0 {
				return 0, nil, nil
			}

			// we have a final, non-terminated record. Return it
			return len(data), data, nil
		}

		if i := bytes.IndexByte(data, delimiter); i >= 0 {
			// We have a delimited record
			return i + 1, data[0:i], nil
		}

		// Request more data.
		return 0, nil, nil
	}
}
