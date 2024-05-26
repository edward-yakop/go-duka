package misc

import (
	"log/slog"
	"os"
)

func SetDefaultLog(level slog.Leveler) {
	slog.SetDefault(
		slog.New(
			slog.NewTextHandler(
				os.Stdout,
				&slog.HandlerOptions{
					Level: level,
				},
			),
		),
	)
}
