package colfmt // import "github.com/mndrix/colfmt"
import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"golang.org/x/crypto/ssh/terminal"
)

type ColumnSpec struct {
	// WidthMin is the minimum allowed width for this column.
	WidthMin int

	// WidthMax is the maximum allowed width for this column.  -1
	// means there is no maximum.
	WidthMax int
}

func (spec *ColumnSpec) HasFlexibleWidth() bool {
	if spec.WidthMax < 0 || spec.WidthMax > spec.WidthMin {
		return true
	}

	return false
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
	for i, spec := range specs {
		fmt.Fprintf(os.Stderr, "%d: %+v ", i, spec)
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
		spec, ok := specs[i]
		if !ok {
			continue
		}

		if width < spec.WidthMin {
			widths[i] = spec.WidthMin
		}
		if spec.WidthMax >= 0 && width > spec.WidthMax {
			widths[i] = spec.WidthMax
		}
	}
	fmt.Fprintf(os.Stderr, "widths = %v\n", widths)
	widths = rebalanceWidths(widths, specs)
	fmt.Fprintf(os.Stderr, "rebalanced = %v\n", widths)

	// create format strings
	formats := make([]string, len(widths))
	for i, width := range widths {
		formats[i] = "%-" + strconv.Itoa(width) + "s"
	}

	// output formatted data
	columns := make([]string, 0, len(widths))
	for _, row := range rows {
		columns = columns[:0] // empty the slice, reusing same memory
		for i, format := range formats {
			if widths[i] == 0 { // skip zero-width columns
				continue
			}
			if len(row[i]) > widths[i] {
				// truncate column
				row[i] = row[i][0:widths[i]]
				//fmt.Fprintf(os.Stderr, "truncated to %q\n", row[i])
			}
			columns = append(columns, fmt.Sprintf(format, row[i]))
		}
		line := strings.Join(columns, outputFieldSeparator)
		io.WriteString(os.Stdout, line)
		io.WriteString(os.Stdout, outputRecordSeparator)
	}
}

func die(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func warn(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
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

func ParseColumnSpecs(specDescription string) (map[int]*ColumnSpec, error) {
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
			specs[n-1] = spec
			if n > maxColumn {
				maxColumn = n
			}
			continue
		}

		// column width in characters like: 7c or 63c
		if width, ok := parseColumnWidth(word); ok {
			spec.WidthMin = width
			spec.WidthMax = width
			continue
		}

		// column width range like: 7c-20c or 10c-*
		if bounds := strings.Split(word, "-"); len(bounds) == 2 {
			warn("  width range: %v", bounds)
			if lower, ok := parseColumnWidth(bounds[0]); ok {
				warn("    lower = %d", lower)
				if upper, ok := parseColumnWidth(bounds[1]); ok {
					warn("    upper = %d", upper)
					spec.WidthMin = lower
					spec.WidthMax = upper
					continue
				}
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

	return specs, nil
}

// returns the width of a column specification, or -1 if the column
// has an infinite width
func parseColumnWidth(word string) (int, bool) {
	// width in characters like: 7c or 42c
	if strings.HasSuffix(word, "c") {
		width, err := strconv.Atoi(strings.TrimSuffix(word, "c"))
		if err == nil {
			return width, true
		}
	}

	// unbounded width like: *
	if word == "*" {
		return -1, true
	}

	return 0, false
}

// adjust widths to fit within a terminal's available horizontal space
func rebalanceWidths(widths []int, specs map[int]*ColumnSpec) []int {
	// how much horizontal space is available?
	availableWidth := 0
	if width, _, err := terminal.GetSize(int(os.Stdout.Fd())); err == nil {
		availableWidth = width
	} else {
		warn("Can't get terminal dimensions: %s", err)
	}

	// how much horizontal space have we consumed?
	consumedWidth := 0
	for i, width := range widths {
		consumedWidth += width
		if i > 0 {
			consumedWidth += 2 // account for gutters
		}
	}

	// which column widths can be adjusted?
	adjustable := make(map[int]*ColumnSpec)
	for i, spec := range specs {
		if spec.HasFlexibleWidth() && widths[i] > spec.WidthMin {
			adjustable[i] = spec
		}
	}

	// reduce widths until everything fits in the space allowed
	warn("rebalancing %d towards %d", consumedWidth, availableWidth)
	for consumedWidth > availableWidth && len(adjustable) > 0 {
		// find the widest adjustable column
		widestIndex := 0
		widestWidth := 0
		for i := range adjustable {
			if widths[i] > widestWidth {
				widestIndex = i
				widestWidth = widths[i]
			}
		}

		// reduce its width by 1 character
		widths[widestIndex]--
		consumedWidth--
		if widths[widestIndex] <= adjustable[widestIndex].WidthMin {
			delete(adjustable, widestIndex)
		}
	}

	return widths
}
