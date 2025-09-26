package runner

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Netflix/go-expect"
	"github.com/acarl005/stripansi"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/terminal"
)

const (
	commentLinePrefix    = "# "
	expectLinePrefix     = "< "
	sendLinePrefix       = "> "
	upArrowContent       = "<up arrow>"
	downArrowContent     = "<down arrow>"
	leftArrowContent     = "<left arrow>"
	rightArrowContent    = "<right arrow>"
	enterContent         = "<enter>"
	spaceContent         = "<space>"
	interactionFilePath  = "interaction.txt"
	interactionLogPrefix = ">>> interaction.txt:line:"
	EOFTimeout           = 30 * time.Second
)

type cmdInputOutput struct {
	stdin        io.Reader
	stdout       io.Writer
	stderr       io.Writer
	debugStdout  io.Writer
	debugStderr  io.Writer
	stdoutBuf    bytes.Buffer
	stderrBuf    bytes.Buffer
	console      terminal.Console
	expectations string
}

type errorHandler func(err error)

func setupCmdInOut(
	ctx context.Context,
	t *testing.T,
	envProvider testhelper.EnvProvider,
	testDirFs filesystem.Fs,
	cmd *exec.Cmd,
	useInteraction bool,
) (*cmdInputOutput, error) {
	t.Helper()
	v := &cmdInputOutput{debugStdout: testhelper.VerboseStdout(), debugStderr: testhelper.VerboseStderr()}
	if useInteraction && testDirFs.IsFile(ctx, interactionFilePath) {
		// Read expectations
		file, err := testDirFs.ReadFile(ctx, filesystem.NewFileDef(interactionFilePath))
		if err != nil {
			t.Fatal(err)
		}

		// Replace ENvs in expectations
		expectations := file.Content
		v.expectations = testhelper.MustReplaceEnvsString(expectations, envProvider)

		// Create virtual terminal
		v.console, err = terminal.New(t, expect.WithStdout(&v.stdoutBuf))
		if err != nil {
			return nil, errors.Errorf("cannot create virtual terminal: %w", err)
		}

		// Setup command
		v.stdin = v.console.TtyRaw()
		v.stdout = v.console.TtyRaw()
		v.stderr = io.MultiWriter(&v.stderrBuf, v.debugStderr)
	} else {
		v.stdin = nil
		v.stdout = io.MultiWriter(&v.stdoutBuf, v.debugStdout)
		v.stderr = io.MultiWriter(&v.stderrBuf, v.debugStderr)
	}

	// Setup cmd
	cmd.Stdin = v.stdin
	cmd.Stdout = v.stdout
	cmd.Stderr = v.stderr
	return v, nil
}

func (v *cmdInputOutput) StdoutString() string {
	return stripansi.Strip(v.stdoutBuf.String())
}

func (v *cmdInputOutput) StderrString() string {
	return stripansi.Strip(v.stderrBuf.String())
}

func (v *cmdInputOutput) InteractAndWait(ctx context.Context, cmd *exec.Cmd, handleErr errorHandler) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(errors.New("interaction completed"))

	// Wait for the command to finish then close interaction
	var cmdErr error
	cmdWg := &sync.WaitGroup{}
	cmdWg.Add(1)
	go func() {
		// Wait for command
		defer cmdWg.Done()
		cmdErr = cmd.Wait()

		// Use context error if any and cmdError is nil
		if err := ctx.Err(); err != nil && cmdErr == nil {
			cmdErr = err
		}

		// Close stdin
		if v.console != nil {
			v.console.TtyRaw().Close()
		}

		// Cancel interaction context
		cancel(errors.New("command finished"))
	}()

	// Skip interaction if it is disabled
	if v.console == nil {
		cmdWg.Wait()
		return cmdErr
	}

	// For each line
	s := bufio.NewScanner(strings.NewReader(v.expectations))
	lineNum := 0
	for s.Scan() {
		// Check context
		select {
		case <-ctx.Done():
			break
		default:
			// continue
		}

		// Get line
		lineNum++
		line := s.Text()

		// Skip empty lines
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}

		// Get line start, first 2 characters
		linePrefix := ""
		if len(line) >= 2 {
			linePrefix = line[0:2]
		} else if len(line) == 1 {
			linePrefix = line + " "
		}

		// Get command data
		content := ""
		if len(line) > 2 {
			content = line[2:]
		}

		if err := v.handleInteraction(lineNum, linePrefix, content); err != nil {
			handleErr(err)
			break
		}
	}

	// Wait for end of stdout
	select {
	case <-ctx.Done():
		// skip waiting
	default:
		if err := v.console.ExpectEOF(expect.WithTimeout(EOFTimeout)); err != nil {
			handleErr(errors.Errorf("interaction: error when waiting for end of stdout: %w", err))
		}
	}

	cmdWg.Wait()
	return cmdErr
}

func (v *cmdInputOutput) handleInteraction(lineNum int, prefix, content string) error {
	// Invoke command according line prefix
	switch prefix {
	case commentLinePrefix:
		// Skip comment
		return nil
	case expectLinePrefix:
		var opts []expect.ExpectOpt

		// Parse duration
		match := regexpcache.MustCompile(`^(\[[a-zA-Z0-9]+\]\s*)`).FindStringSubmatch(content)
		if len(match) > 1 {
			durationStr := strings.Trim(match[1], " []")
			content = strings.TrimPrefix(content, match[1])
			duration, err := time.ParseDuration(durationStr)
			if err != nil {
				return v.errorf(lineNum, `could parse duration %+q from expectation %+q: %w`, durationStr, content, err)
			}
			opts = append(opts, expect.WithTimeout(duration))
		}

		// Wait for expected string
		v.logf("%d: expecting %+q", lineNum, content)
		if err := v.console.ExpectString(content, opts...); err != nil {
			return v.errorf(lineNum, `could not meet expectation %+q: %w`, content, err)
		}
	case sendLinePrefix:
		v.logf(`%d: sending %+q`, lineNum, content)
		switch strings.TrimSpace(content) {
		case upArrowContent:
			if err := v.console.SendUpArrow(); err != nil {
				return v.errorf(lineNum, "could send up arrow: %w", err)
			}
		case downArrowContent:
			if err := v.console.SendDownArrow(); err != nil {
				return v.errorf(lineNum, "could send down arrow: %w", err)
			}
		case leftArrowContent:
			if err := v.console.SendLeftArrow(); err != nil {
				return v.errorf(lineNum, "could send left arrow: %w", err)
			}
		case rightArrowContent:
			if err := v.console.SendRightArrow(); err != nil {
				return v.errorf(lineNum, "could send right arrow: %w", err)
			}
		case enterContent:
			if err := v.console.SendEnter(); err != nil {
				return v.errorf(lineNum, "could send enter: %w", err)
			}
		case spaceContent:
			if err := v.console.SendSpace(); err != nil {
				return v.errorf(lineNum, "could send space: %w", err)
			}
		default:
			if err := v.console.SendLine(content); err != nil {
				return v.errorf(lineNum, "could send input %+q: %w", content, err)
			}
		}
	default:
		return v.errorf(
			lineNum,
			"line must start with %+q for expectation and %+q for send command, found %+q",
			expectLinePrefix, sendLinePrefix, prefix,
		)
	}

	return nil
}

func (v *cmdInputOutput) logf(format string, args ...any) {
	format = "\n\n" + interactionLogPrefix + format + "\n\n"
	_, _ = v.debugStdout.Write(fmt.Appendf(nil, format, args...))
}

func (v *cmdInputOutput) errorf(lineNum int, format string, args ...any) error {
	args = append([]any{lineNum}, args...)
	format = interactionLogPrefix + "%d:" + format
	return errors.Errorf(format, args...)
}
