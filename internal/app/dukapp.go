package app

import (
	"fmt"
	"github.com/ed-fx/go-duka/api/instrument"
	"github.com/ed-fx/go-duka/api/tickdata"
	iTickdata "github.com/ed-fx/go-duka/internal/tickdata"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ed-fx/go-duka/internal/core"
	"github.com/ed-fx/go-duka/internal/export/csv"
	"github.com/ed-fx/go-duka/internal/export/fxt4"
	"github.com/ed-fx/go-duka/internal/export/hst"
	"github.com/ed-fx/go-duka/internal/misc"
)

var (
	log             = misc.NewLogger("App", 2)
	supportsFormats = []string{"csv", "fxt", "hst"}
)

type ArgsList struct {
	Verbose bool
	Header  bool
	Spread  uint
	Model   uint
	Dump    string
	Symbol  string
	Output  string
	Format  string
	Period  string
	Start   string
	End     string
}

// DukaApp used to download source tick data
//
type DukaApp struct {
	option  AppOption
	outputs []core.Converter
}

// AppOption download options
//
type AppOption struct {
	Start      time.Time
	End        time.Time
	Instrument *instrument.Metadata
	Format     string
	Folder     string
	Periods    string
	Spread     uint32
	Mode       uint32
	CsvHeader  bool
}

// ParseOption parse input command line
//
func ParseOption(args ArgsList) (*AppOption, error) {
	metadata := instrument.GetMetadata(args.Symbol)
	var err error
	opt := AppOption{
		CsvHeader:  args.Header,
		Format:     args.Format,
		Instrument: metadata,
		Spread:     uint32(args.Spread),
		Mode:       uint32(args.Model),
	}

	if metadata == nil {
		err = fmt.Errorf("invalid symbol parameter [%s]", args.Symbol)
		return nil, err
	}
	// check format
	{
		bSupport, format := false, strings.ToLower(args.Format)
		for _, sformat := range supportsFormats {
			if format == sformat {
				bSupport = true
				break
			}
		}
		if !bSupport {
			err = fmt.Errorf("not supported output format")
			return nil, err
		}
		opt.Format = format
	}
	if err = handleTimeArguments(args, &opt); err != nil {
		return nil, err
	}
	if opt.Folder, err = filepath.Abs(args.Output); err != nil {
		err = fmt.Errorf("invalid destination folder")
		return nil, err
	}
	if err = os.MkdirAll(opt.Folder, 666); err != nil {
		err = fmt.Errorf("create destination folder failed: %v", err)
		return nil, err
	}

	if args.Period != "" {
		args.Period = strings.ToUpper(args.Period)
		if !core.TimeframeRegx.MatchString(args.Period) {
			err = fmt.Errorf("invalid timeframe value: %s", args.Period)
			return nil, err
		}
		opt.Periods = args.Period
	}

	return &opt, nil
}

func handleTimeArguments(args ArgsList, opt *AppOption) (err error) {
	if opt.Start, err = parseDateArgument(args.Start); err != nil {
		err = errors.Wrap(err, "invalid start parameter")
		return
	}
	if opt.End, err = parseDateArgument(args.End); err != nil {
		err = fmt.Errorf("invalid end parameter")
		return
	}
	if opt.End.Before(opt.Start) || opt.End.Equal(opt.Start) {
		err = fmt.Errorf("invalid end parameter which shouldn't early then start")
		return
	}
	return
}

func parseDateArgument(dateString string) (time.Time, error) {
	return time.ParseInLocation("2006-01-02", dateString, time.UTC)
}

// NewOutputs create timeframe instance
//
func NewOutputs(opt *AppOption) []core.Converter {
	outs := make([]core.Converter, 0)
	for _, period := range strings.Split(opt.Periods, ",") {
		var format core.Converter
		timeframe, _ := core.ParseTimeframe(strings.Trim(period, " \t\r\n"))

		switch opt.Format {
		case "csv":
			format = csv.New(opt.Start, opt.End, opt.CsvHeader, opt.Instrument, opt.Folder)
			break
		case "fxt":
			format = fxt4.NewFxtFile(timeframe, opt.Spread, opt.Mode, opt.Folder, opt.Instrument)
			break
		case "hst":
			format = hst.NewHST(timeframe, opt.Spread, opt.Instrument, opt.Folder)
			break
		default:
			log.Error("unsupported format %s.", opt.Format)
			return nil
		}

		outs = append(outs, core.NewTimeframe(period, opt.Instrument, format))
	}
	return outs
}

// NewApp create an application instance by input arguments
//
func NewApp(opt *AppOption) *DukaApp {
	return &DukaApp{
		option:  *opt,
		outputs: NewOutputs(opt),
	}
}

// Execute download source bi5 tick data from dukascopy
//
func (app *DukaApp) Execute() error {
	var (
		opt       = app.option
		startTime = time.Now()
	)

	if len(app.outputs) < 1 {
		log.Error("No valid output format")
		return errors.New("no valid output format")
	}

	// Create an output directory
	if _, err := os.Stat(opt.Folder); os.IsNotExist(err) {
		if err = os.MkdirAll(opt.Folder, 0770); err != nil {
			log.Error("Create folder (%s) failed: %v.", opt.Folder, err)
			return err
		}
	}

	// Download by day, 24 hours a day data is downloaded in parallel by 24 goroutines
	for day := opt.Start; day.Unix() < opt.End.Unix(); day = day.Add(24 * time.Hour) {
		// Download, parse, store
		if td, err := iTickdata.FetchDay(opt.Instrument, day, opt.Folder); err != nil {
			err = errors.Wrap(err, "Failed to fetch ["+misc.TimeToDayString(day)+"]")
			return err
		} else if err = app.export(td); err != nil {
			err = errors.Wrap(err, "Failed to export ["+misc.TimeToDayString(day)+"]")
		}
	}

	//  flush all output file
	var wg sync.WaitGroup
	for _, output := range app.outputs {
		wg.Add(1)
		go func(o core.Converter) {
			defer wg.Done()
			_ = o.Finish()
		}(output)
	}

	wg.Wait()
	log.Info("Time cost: %v.", time.Since(startTime))
	return nil
}

// export
func (app *DukaApp) export(td tickdata.Day) error {
	day := td.Time()
	dayTicks := make([]*tickdata.TickData, 0, 2048)
	td.EachDay(func(ticks []*tickdata.TickData, err error) bool {
		if err != nil {
			log.Error("Decode bi5 %s: %s failed: %v.", td.Symbol(), day.Format("2006-01-02:15H"), err)
		} else {
			dayTicks = append(dayTicks, ticks...)
		}
		return true
	})

	timestamp := uint32(day.Unix())
	for _, out := range app.outputs {
		err := out.PackTicks(timestamp, dayTicks[:])
		if err != nil {
			return errors.Wrap(err, "Generating output ["+day.Format("2006-01-02:15H")+"]")
		}
	}

	return nil
}
