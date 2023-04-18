package spinner

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"
)

const DEFAULT_REFRESH_INTERVAL = time.Second / 10
const DEFAULT_CHARSET = "|/-\\"

type Spinner struct {
	ch          chan cmd
	done        chan struct{}
	running     atomic.Bool
	output      io.Writer
	text        string
	shouldClear bool
	interval    time.Duration
	charset     []rune
	step        int
}

type option func(*Spinner)

func WithText(text string) option {
	return func(s *Spinner) {
		s.SetText(text)
	}
}

func WithOutput(output io.Writer) option {
	return func(s *Spinner) {
		s.SetOutput(output)
	}
}

func WithCharset(charset string) option {
	return func(s *Spinner) {
		s.SetCharset(charset)
	}
}

func WithRefreshInterval(interval time.Duration) option {
	return func(s *Spinner) {
		s.SetRefreshInterval(interval)
	}
}

func WithClear(clear bool) option {
	return func(s *Spinner) {
		s.SetClear(clear)
	}
}

func New(opts ...option) *Spinner {
	spinner := &Spinner{
		ch:          make(chan cmd),
		done:        make(chan struct{}),
		running:     atomic.Bool{},
		output:      os.Stdout,
		text:        "",
		charset:     []rune(DEFAULT_CHARSET),
		shouldClear: false,
		interval:    DEFAULT_REFRESH_INTERVAL,
	}

	for _, opt := range opts {
		opt(spinner)
	}

	return spinner
}

func (s *Spinner) Start() *Spinner {
	if s.running.Load() {
		return s
	}
	go s.run()
	s.running.Store(true)

	return s
}

func (s *Spinner) Step(text string) {
	s.Stop()
	s.SetText(text)
	s.Start()
}

func (s *Spinner) sendCmd(cmd cmd) {
	if !s.running.Load() {
		cmd.apply(s)
	} else {
		s.ch <- cmd
	}
}

type cmd interface {
	apply(s *Spinner)
}

type changeOutputCmd struct {
	output io.Writer
}

func (cmd *changeOutputCmd) apply(s *Spinner) {
	s.output = cmd.output
}

func (s *Spinner) SetOutput(output io.Writer) {
	s.sendCmd(&changeOutputCmd{output})
}

type changeCharsetCmd struct {
	charset []rune
}

func (cmd *changeCharsetCmd) apply(s *Spinner) {
	s.charset = cmd.charset
	s.step = 0
}

func (s *Spinner) SetCharset(charset string) {
	s.sendCmd(&changeCharsetCmd{charset: []rune(charset)})
}

type changeClearCmd struct {
	clear bool
}

func (cmd *changeClearCmd) apply(s *Spinner) {
	s.shouldClear = cmd.clear
}

func (s *Spinner) SetClear(clear bool) {
	s.sendCmd(&changeClearCmd{clear})
}

type changeRefreshIntervalCmd struct {
	interval time.Duration
}

func (cmd *changeRefreshIntervalCmd) apply(s *Spinner) {
	s.interval = cmd.interval
}

func (s *Spinner) SetRefreshInterval(interval time.Duration) {
	s.sendCmd(&changeRefreshIntervalCmd{interval})
}

type changeTextCmd struct {
	text string
}

func (cmd *changeTextCmd) apply(s *Spinner) {
	s.text = cmd.text
}

func (s *Spinner) SetText(text string) {
	s.sendCmd(&changeTextCmd{text})
}

type stopCmd struct{}

func (cmd *stopCmd) apply(s *Spinner) {
	s.running.Store(false)
}

func (s *Spinner) Stop() {
	if !s.running.Load() {
		return
	}

	s.sendCmd(&stopCmd{})
	<-s.done
}

func (s *Spinner) run() {
	for {
		for {
			select {
			case cmd := <-s.ch:
				cmd.apply(s)
				continue
			default:
			}
			break
		}

		if !s.running.Load() {
			s.clear()
			s.done <- struct{}{}
			return
		}

		s.render()
		<-s.frame()
	}
}

func (s *Spinner) render() {
	fmt.Fprintf(s.output, "\033[2K\n\033[1A%s %s", string(s.charset[s.step]), s.text)
	s.step = (s.step + 1) % len(s.charset)
}

func (s *Spinner) clear() {
	if s.shouldClear {
		fmt.Fprintf(s.output, "\033[2K\r")
	} else {
		fmt.Fprintf(s.output, "\033[2K\r✓ %s\n", s.text)
	}
}

func (s *Spinner) frame() <-chan time.Time {
	return time.After(s.interval)
}
