package main

import (
	"ed-fx/go-duka/app"
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"ed-fx/go-duka/fxt4"
	"github.com/go-clog/clog"
)

func init() {
	/*
		var fpath string
		if logPath == "" {
			fpath, _ = os.Getwd()
		} else {
			fpath, _ = filepath.Abs(logPath)
		}

		if err := os.MkdirAll(filepath.Dir(fpath), 666); err != nil {
			fmt.Printf("[App] Create log folder failed: %v.", err)
			os.Exit(-1)
		}
		log.Trace("App Path: %s.", fpath)

		log.New(log.FILE, log.FileConfig{
			Level:      log.TRACE,
			Filename:   filepath.Join(fpath, "app.log"),
			BufferSize: 2048,
			FileRotationConfig: log.FileRotationConfig{
				Rotate:  true,
				MaxDays: 30,
				MaxSize: 50 * (1 << 20),
			},
		})
	*/
}

func main() {
	args := app.ArgsList{}
	start := time.Now().Format("2006-01-02")
	end := time.Now().Add(24 * time.Hour).Format("2006-01-02")
	flag.StringVar(&args.Dump,
		"dump", "",
		"dump given file format")
	flag.StringVar(&args.Period,
		"timeframe", "M1",
		"timeframe values: M1, M5, M15, M30, H1, H4, D1, W1, MN (Comma separated list)")
	flag.StringVar(&args.Symbol,
		"symbol", "",
		"symbol list using format, like: EURUSD EURGBP (*required)")
	flag.StringVar(&args.Start,
		"start", start,
		"start date format YYYY-MM-DD")
	flag.StringVar(&args.End,
		"end", end,
		"end date format YYYY-MM-DD")
	flag.StringVar(&args.Output,
		"output", ".",
		"destination directory to save the output file")
	flag.UintVar(&args.Spread,
		"spread", 20,
		"spread value in points")
	flag.UintVar(&args.Model,
		"model", 0,
		"one of the model values: 0, 1, 2")
	flag.StringVar(&args.Format,
		"format", "",
		"output file format, supported csv/hst/fxt (*required)")
	flag.BoolVar(&args.Header,
		"header", false,
		"save csv with header")
	flag.BoolVar(&args.Local,
		"local", true, // Always use local data if available
		"convert to given format with local data instead of downloading from dukascopy")
	flag.BoolVar(&args.Verbose,
		"verbose", false,
		"verbose output trace log")
	flag.Parse()

	if args.Verbose {
		clog.New(clog.CONSOLE, clog.ConsoleConfig{
			Level:      clog.TRACE,
			BufferSize: 100,
		})
	} else {
		clog.New(clog.CONSOLE, clog.ConsoleConfig{
			Level:      clog.INFO,
			BufferSize: 100,
		})
	}

	if args.Dump != "" {
		if filepath.Ext(args.Dump) == ".fxt" {
			fxt4.DumpFile(args.Dump, args.Header, nil)
		} else {
			fmt.Println("invalid file ext", filepath.Ext(args.Dump))
		}
		return
	}

	opt, err := app.ParseOption(args)
	if err != nil {
		fmt.Println("--------------------------------------------")
		fmt.Printf("Error: %s\n", err)
		fmt.Println("--------------------------------------------\n")
		fmt.Println("Usage:")
		flag.PrintDefaults()
		return
	}

	fmt.Printf("    Output: %s\n", opt.Folder)
	fmt.Printf("    Symbol: %s\n", opt.Symbol)
	fmt.Printf("    Spread: %d\n", opt.Spread)
	fmt.Printf("      Mode: %d\n", opt.Mode)
	fmt.Printf(" Timeframe: %s\n", opt.Periods)
	fmt.Printf("    Format: %s\n", opt.Format)
	fmt.Printf(" CsvHeader: %t\n", opt.CsvHeader)
	fmt.Printf(" LocalData: %t\n", opt.Local)
	fmt.Printf(" StartDate: %s\n", opt.Start.Format("2006-01-02:15H"))
	fmt.Printf("   EndDate: %s\n", opt.End.Format("2006-01-02:15H"))

	defer clog.Shutdown()
	app := app.NewApp(opt)
	app.Execute()
}
