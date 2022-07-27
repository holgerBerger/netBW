package main

import (
	"fmt"
	flags "github.com/jessevdk/go-flags"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pub"
	"go.nanomsg.org/mangos/v3/protocol/surveyor"
	//"go.nanomsg.org/mangos/v3/protocol/sub"

	// register transports
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

// command line options
var opts struct {
	Time        int      `long:"time" short:"t" default:"10" description:"minimum time [s] to run test for"`
	Source      []string `long:"source" short:"s" description:"source hosts"`
	Destination []string `long:"destination" short:"d" description:"destination hosts"`
	Verbose     bool     `long:"verbose" short:"v" description:"be more verbose"`
	Sender      bool     `long:"sender" hidden:"true"`
	Receiver    bool     `long:"receiver" hidden:"true"`
	Commander   string   `long:"commander" hidden:"true" description:"commander url for workers"`
	Survey      string   `long:"survey" hidden:"true" description:"survey url for workers"`
	Basesocket  int      `long:"basesocket" default:"44444" description:"lowest socket number on receiver"`
	Sockets     int      `long:"sockets" hidden:"true" description:"sockets to open for a receiver"`
	Index       int      `long:"index" hidden:"true" description:"index in source list"`
}

var cmdurl, surveyurl string

func main() {
	path := os.Args[0]
	hostname, err := os.Hostname()
	if err != nil {
		log.Println("can not find hostname")
		os.Exit(1)
	}
	cmdurl = "tcp://" + hostname + ":7890"
	surveyurl = "tcp://" + hostname + ":7891"

	parser := flags.NewParser(&opts, flags.Default)
	parser.Usage = "[OPTIONS] -s h1,h2 -d h3,h4"
	_, err = parser.Parse()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// are we a sender or receiver
	if opts.Sender || opts.Receiver {
		cmdurl = opts.Commander
		surveyurl = opts.Survey
		var f *os.File

		// direct logger to file
		if opts.Sender {
			f, err = os.OpenFile(fmt.Sprintf("/tmp/netBW-sender-%s.log", hostname), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		} else {
			f, err = os.OpenFile(fmt.Sprintf("/tmp/netBW-receiver-%s.log", hostname), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		}
		if err != nil {
			log.Fatalf("error opening file: %v", err)
		}
		wrt := io.MultiWriter(os.Stdout, f)
		log.SetOutput(wrt)
	}
	if opts.Sender {
		sender()
	} else if opts.Receiver {
		receiver()
	} else {

		// main on commander

		if len(opts.Source) == 0 || len(opts.Destination) == 0 {
			fmt.Println("need source and destination host lists")
			os.Exit(1)
		}
		if len(opts.Source) != len(opts.Destination) {
			fmt.Println("number of source and destinantion hosts not equal!")
			os.Exit(1)
		}
		fmt.Println("Running with", len(opts.Source)*len(opts.Destination), "streams")

		// create pub/sub publisher
		var pubsock mangos.Socket
		var err error

		if pubsock, err = pub.NewSocket(); err != nil {
			log.Panic("can't get new pub socket: %s", err)
		}
		if err = pubsock.Listen(cmdurl); err != nil {
			log.Panic("can't listen on pub socket: %s", err.Error())
		}

		// create survey
		var surveysock mangos.Socket
		if surveysock, err = surveyor.NewSocket(); err != nil {
			log.Panic("can't get new surveyor socket: %s", err)
		}
		if err = surveysock.Listen(surveyurl); err != nil {
			log.Panic("can't listen on surveyor socket: %s", err.Error())
		}
		err = surveysock.SetOption(mangos.OptionSurveyTime, time.Second*2)
		if err != nil {
			log.Panic("SetOption(): %s", err.Error())
		}

		// starting receivers and senders
		for _, d := range opts.Destination {
			log.Println("spawn receiver on", d)
			go spawnProcess(d, "--receiver --sockets="+strconv.Itoa(len(opts.Source)), path, cmdurl, surveyurl, strconv.Itoa(opts.Basesocket))
		}

		dstlist := " "
		for _, d := range opts.Destination {
			dstlist += " -d " + d
		}

		for index, s := range opts.Source {
			log.Println("spawn sender on", s)
			go spawnProcess(s, "--sender --index="+strconv.Itoa(index)+dstlist, path, cmdurl, surveyurl, strconv.Itoa(opts.Basesocket))
		}

		// wait a bit for startup
		time.Sleep(10 * time.Second)

		// start benmark
		log.Println("starting benchmark")
		if err = pubsock.Send([]byte("START")); err != nil {
			log.Panic("Failed publishing: %s", err.Error())
		}

		// run benchmark
		time.Sleep(time.Duration(opts.Time) * time.Second)

		// stop it
		log.Println("stopping benchmark")
		if err = pubsock.Send([]byte("STOP")); err != nil {
			log.Panic("Failed publishing: %s", err.Error())
		}

		log.Println("waiting")
		time.Sleep(5 * time.Second)

		// get results
		log.Println("getting results")
		if err = surveysock.Send([]byte("RESULT")); err != nil {
			log.Panic("Failed sending survey: %s", err.Error())
		}
		time.Sleep(1 * time.Second)

		resultmatrix := make(map[string]map[string]int)
		for _, sender := range opts.Source {
			resultmatrix[strings.Split(sender, ".")[0]] = make(map[string]int)
		}

		var bytes int64
		for {
			var msg []byte
			if msg, err = surveysock.Recv(); err != nil {
				//log.Println(err)
				break
			}
			//log.Printf("SERVER: RECEIVED \"%s\" SURVEY RESPONSE\n", string(msg))
			lines := strings.Split(string(msg), "\n")
			for _, line := range lines {
				//log.Println("line:", line)
				fields := strings.Split(line, ":")
				if len(fields) < 3 {
					continue
				}
				//log.Println("fields:", fields, len(fields))
				value, err := strconv.Atoi(fields[2])
				if err != nil {
					log.Println(err, line)
				}
				bytes += int64(value)
				resultmatrix[fields[0]][strings.Split(fields[1], ".")[0]] = value
			}
		}

		// print results
		GBs := (float64(bytes) / (1000.0 * 1000.0 * 1000.0)) / float64(opts.Time)
		GiBs := (float64(bytes) / (1024.0 * 1024.0 * 1024.0)) / float64(opts.Time)
		log.Println("===============================================================")
		log.Println("result: ", GBs, "GB/s  ", GBs/float64(len(opts.Source)), "per sending node")
		log.Println("        ", GiBs, "GiB/s  ", GiBs/float64(len(opts.Source)), "per sending node")
		log.Println("        ", GBs*8, "Gb/s  ", 8*GBs/float64(len(opts.Source)), "per sending node")
		log.Println("        ", GiBs*8, "Gib/s  ", 8*GiBs/float64(len(opts.Source)), "per sending node")
		log.Println("===============================================================")
		out := fmt.Sprintf("%15s", "Gbit/s")
		for _, dest := range opts.Destination {
			out += fmt.Sprintf("%15s", dest)
		}
		log.Println(out)
		for _, sender := range opts.Source {
			out = fmt.Sprintf("%15s", sender)
			for _, dest := range opts.Destination {
				out += fmt.Sprintf("%15f", (float64(resultmatrix[strings.Split(sender, ".")[0]][strings.Split(dest, ".")[0]]*8)/(1000*1000*1000))/float64(opts.Time))
			}
			log.Println(out)
		}

		// exit programm

		if err = pubsock.Send([]byte("EXIT")); err != nil {
			log.Panic("Failed publishing: %s", err.Error())
		}
	}
}
