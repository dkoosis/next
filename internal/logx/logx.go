package logx

import (
	"context"
	"log/slog"
)

func log(ctx context.Context, level slog.Level, msg string, err error, args ...any) {
	if ctx == nil {
		ctx = context.Background()
	}
	attrs := make([]any, 0, len(args)+2)
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	attrs = append(attrs, args...)
	slog.Default().Log(ctx, level, msg, attrs...)
}

func Error(ctx context.Context, msg string, err error, args ...any) {
	log(ctx, slog.LevelError, msg, err, args...)
}

func Warn(ctx context.Context, msg string, err error, args ...any) {
	log(ctx, slog.LevelWarn, msg, err, args...)
}

func Info(ctx context.Context, msg string, args ...any) {
	log(ctx, slog.LevelInfo, msg, nil, args...)
}

func Debug(ctx context.Context, msg string, args ...any) {
	log(ctx, slog.LevelDebug, msg, nil, args...)
}
