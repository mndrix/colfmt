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

type ColumnSpec struct {
	// WidthMin is the minimum allowed width for this column.
	WidthMin int

	// WidthMax is the maximum allowed width for this column.  -1
	// means there is no maximum.
	WidthMax int
}

func Main() {
	var inputRecordSeparator byte = '\n'
	var inputFieldSeparator byte = '\t'
	outputRecordSeparator := "\n"
	outputFieldSeparator := "  "

	// parse column specification
	rawSpec := ""
	if len(os.Args) > 1 {
		rawSpec = os.Args[1]
	}
	specs, err := ParseColumnSpecs(rawSpec)
	if err != nil {
		die("parsing column spec: %s", err)
	}
	fmt.Fprintf(os.Stderr, "specs = ")
	for _, spec := range specs {
		fmt.Fprintf(os.Stderr, "%+v ", spec)
	}
	fmt.Fprintf(os.Stderr, "\n")

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

	// adjust column widths based on specs
	for i, width := range widths {
		if i >= len(specs) || specs[i] == nil {
			continue
		}

		spec := specs[i]
		if width < spec.WidthMin {
			widths[i] = spec.WidthMin
		}
		if width > spec.WidthMax {
			widths[i] = spec.WidthMax
		}
	}
	fmt.Fprintf(os.Stderr, "widths = %v\n", widths)

	// create format strings
	formats := make([]string, len(widths))
	for i, width := range widths {
		formats[i] = "%-" + strconv.Itoa(width) + "s"
	}

	// output formatted data
	columns := make([]string, len(widths))
	for _, row := range rows {
		for i, format := range formats {
			if len(row[i]) > widths[i] {
				// truncate column
				row[i] = row[i][0:widths[i]]
				fmt.Fprintf(os.Stderr, "truncated to %q\n", row[i])
			}
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

func ParseColumnSpecs(specDescription string) ([]*ColumnSpec, error) {
	// map column number to the associated spec
	specs := make(map[int]*ColumnSpec)
	maxColumn := 0

	// parse each word of the spec description
	scan := bufio.NewScanner(strings.NewReader(specDescription))
	scan.Split(bufio.ScanWords)
	spec := &ColumnSpec{}
	needNewSpec := false
	for scan.Scan() {
		if needNewSpec {
			spec = &ColumnSpec{}
			needNewSpec = false
		}

		word := scan.Text()
		fmt.Fprintf(os.Stderr, "parsing %q\n", word)
		if strings.HasSuffix(word, ";") {
			needNewSpec = true
			word = strings.TrimSuffix(word, ";")
			fmt.Fprintf(os.Stderr, "  now %q\n", word)
		}

		// column number like: 6 or 1 or 999
		if n, err := strconv.Atoi(word); err == nil {
			if n < 1 {
				return nil, fmt.Errorf("invalid column number: %d", n)
			}
			specs[n] = spec
			if n > maxColumn {
				maxColumn = n
			}
			continue
		}

		// column width in characters like: 7c or 63c
		if strings.HasSuffix(word, "c") {
			width, err := strconv.Atoi(strings.TrimSuffix(word, "c"))
			if err == nil {
				spec.WidthMin = width
				spec.WidthMax = width
				continue
			}
		}

		// keywords
		switch word {
		case ";":
			needNewSpec = true
		default:
			return nil, fmt.Errorf("unexpected token: %s", word)
		}
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}

	allSpecs := make([]*ColumnSpec, maxColumn)
	for n, spec := range specs {
		allSpecs[n-1] = spec
	}
	return allSpecs, nil
}
