package app

import (
	"fmt"
	"os"
	"testing"
)

func TestDukaApp(t *testing.T) {
	t.Cleanup(func() {
		_ = os.RemoveAll("EURUSD-2017-01-01-2017-01-03.CSV")
		_ = os.RemoveAll("download")
	})
	args := ArgsList{
		Verbose: true,
		Header:  true,
		Spread:  20,
		Model:   0,
		Symbol:  "EURUSD",
		Format:  "csv",
		Period:  "M1",
		Start:   "2017-01-01",
		End:     "2017-01-03",
	}

	opt, err := ParseOption(args)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("    Output: %s\n", opt.Folder)
	fmt.Printf("Instrument: %s\n", opt.Instrument.Code())
	fmt.Printf("    Spread: %d\n", opt.Spread)
	fmt.Printf("      Mode: %d\n", opt.Mode)
	//fmt.Printf(" Timeframe: %d\n", opt.Timeframe)
	fmt.Printf("    Format: %s\n", opt.Format)
	fmt.Printf(" CsvHeader: %t\n", opt.CsvHeader)
	fmt.Printf(" StartDate: %s\n", opt.Start.Format("2006-01-02:15H"))
	fmt.Printf("   EndDate: %s\n", opt.End.Format("2006-01-02:15H"))

	app := NewApp(opt)
	_ = app.Execute()
}
