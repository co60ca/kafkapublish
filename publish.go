package kafkapublish

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

func run(args []string) error {
	if len(args) < 1 {
		panic("missing arg 0")
	}

	var fs flag.FlagSet
	fs.Usage = func() {
		_, _ = fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] broker topic [msg]\n", args[0])
		fs.PrintDefaults()
	}
	var file string
	fs.StringVar(&file, "file", "", "filename of file containing message")

	fs.Parse(args[1:])

	var data []byte
	if file != "" {
		bdata, err := ioutil.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", file, err)
		}
		data = bdata
	}
	if len(data) == 0 {
		if fs.NArg() != 3 || fs.Arg(2) == "" {
			fs.Usage()
			return errors.New("missing message parameter and -file is empty")
		}
		data = []byte(fs.Arg(2))
	}
	broker := fs.Arg(0)
	topic := fs.Arg(1)

	if broker == "" {
		fs.Usage()
		return errors.New("broker not set")
	}
	if topic == "" {
		fs.Usage()
		return errors.New("topic not set")
	}

	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		return err
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(kafka.Message{Value: data})
	if err != nil {
		return fmt.Errorf("failed to write messages: %w", err)
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}
	return nil
}

func Main() {
	err := run(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal: %s\n", err)
		os.Exit(1)
	}
}
