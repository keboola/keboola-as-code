package cli

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"testing"

	"github.com/Netflix/go-expect"
	"github.com/acarl005/stripansi"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/terminal"
)

const commentLinePrefix = "# "
const expectLinePrefix = "< "
const sendLinePrefix = "> "
const upArrowContent = "<up arrow>"
const downArrowContent = "<down arrow>"
const leftArrowContent = "<left arrow>"
const rightArrowContent = "<right arrow>"
const enterContent = "<enter>"
const interactionFilePath = "interaction.txt"
const interactionLogPrefix = ">>> interaction.txt:line:"

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

func setupCmdInOut(t *testing.T, envProvider testhelper.EnvProvider, testDirFs filesystem.Fs, cmd *exec.Cmd) (*cmdInputOutput, error) {
	t.Helper()
	v := &cmdInputOutput{debugStdout: testhelper.VerboseStdout(), debugStderr: testhelper.VerboseStderr()}
	if testDirFs.IsFile(interactionFilePath) {
		// Read expectations
		file, err := testDirFs.ReadFile(filesystem.NewFileDef(interactionFilePath))
		if err != nil {
			t.Fatal(err)
		}

		// Replace ENvs in expectations
		expectations := file.Content
		v.expectations = testhelper.MustReplaceEnvsString(expectations, envProvider)

		// Create virtual terminal
		v.console, err = terminal.New(t, expect.WithStdout(&v.stdoutBuf))
		if err != nil {
			return nil, fmt.Errorf("cannot create virtual terminal: %w", err)
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

func (v *cmdInputOutput) InteractAndWait(cmd *exec.Cmd, handleErr errorHandler) error {
	// Check if interaction is enabled
	if v.console == nil {
		return cmd.Wait()
	}

	// For each line
	s := bufio.NewScanner(strings.NewReader(v.expectations))
	lineNum := 0
	for s.Scan() {
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

	err := cmd.Wait()
	v.console.TtyRaw().Close()

	// Wait for end of stdout
	if err := v.console.ExpectEOF(); err != nil {
		handleErr(fmt.Errorf("interaction: error when waiting for end of stdout: %w", err))
	}

	return err
}

func (v *cmdInputOutput) handleInteraction(lineNum int, prefix, content string) error {
	// Invoke command according line prefix
	switch prefix {
	case commentLinePrefix:
		// Skip comment
		return nil
	case expectLinePrefix:
		v.logf("%d: expecting %+q", lineNum, content)
		if err := v.console.ExpectString(content); err != nil {
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
				return v.errorf(lineNum, "could send ender: %w", err)
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
	_, _ = v.debugStdout.Write([]byte(fmt.Sprintf(format, args...)))
}

func (v *cmdInputOutput) errorf(lineNum int, format string, args ...any) error {
	args = append([]any{lineNum}, args...)
	format = interactionLogPrefix + "%d:" + format
	return fmt.Errorf(format, args...)
}
