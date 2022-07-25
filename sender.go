package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"go.nanomsg.org/mangos/v3"
	//"go.nanomsg.org/mangos/v3/protocol/pub"
	"go.nanomsg.org/mangos/v3/protocol/respondent"
	"go.nanomsg.org/mangos/v3/protocol/sub"

	// register transports
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

func sender() {
	var subsock mangos.Socket
	var err error
	var msg []byte
	var bytesmutex sync.Mutex
	var byteswg sync.WaitGroup

	var stopflag bool
	var stopmutex sync.RWMutex

	stopmutex.Lock()
	stopflag = false
	stopmutex.Unlock()

	totalbytes := make([]int64, len(opts.Destination), len(opts.Destination))

	// create subscriber
	if subsock, err = sub.NewSocket(); err != nil {
		log.Panic("can't get new sub socket: %s", err.Error())
	}
	if err = subsock.Dial(cmdurl); err != nil {
		log.Panic("can't dial on sub socket: %s", err.Error())
	}
	// Empty byte array effectively subscribes to everything
	err = subsock.SetOption(mangos.OptionSubscribe, []byte(""))
	if err != nil {
		log.Panic("cannot subscribe: %s", err.Error())
	}

	// create responder
	var respondersock mangos.Socket

	if respondersock, err = respondent.NewSocket(); err != nil {
		log.Panic("can't get new respondent socket: %s", err.Error())
	}
	if err = respondersock.Dial(surveyurl); err != nil {
		log.Panic("can't dial on respondent socket: %s", err.Error())
	}

	hostname, _ := os.Hostname()

	for {
		if msg, err = subsock.Recv(); err != nil {
			log.Panic("Cannot recv: %s", err.Error())
		}
		if string(msg) == "START" {
			log.Println("start with", opts.Destination)
			for di, d := range opts.Destination {
				go func(di int, dest string) {
					byteswg.Add(1)
					log.Print("connecting: ", dest+":"+strconv.Itoa(opts.Index+opts.Basesocket))
					connection, err := net.Dial("tcp", dest+":"+strconv.Itoa(opts.Index+opts.Basesocket))
					if err != nil {
						panic(err)
					}
					defer connection.Close()
					/*
						err = connection.SetWriteBuffer(1024 * 1024)
						err = connection.SetReadBuffer(1024 * 1024)
						if err != nil {
							log.Println("could not set socket buffer size to 1MiB")
						}
					*/
					///send some data
					buffer := make([]byte, 1024*1024)
					t1 := time.Now()
					bytes := int64(0)
					for {
						b, _ := connection.Write(buffer)
						bytes += int64(b)

						stopmutex.RLock()
						if stopflag {
							stopmutex.RUnlock()
							break
						}
						stopmutex.RUnlock()
					}
					t2 := time.Now()
					log.Println(hostname, "->", dest, " byte/sec:", float64(bytes)/t2.Sub(t1).Seconds())
					bytesmutex.Lock()
					totalbytes[di] = bytes
					bytesmutex.Unlock()
					byteswg.Done()
				}(di, d)
			}
		}
		if string(msg) == "STOP" {
			log.Println("stopping benchmark")
			stopmutex.Lock()
			stopflag = true
			stopmutex.Unlock()

			// wait for survey and answer
			if msg, err = respondersock.Recv(); err != nil {
				log.Panic("Cannot recv: %s", err.Error())
			}
			log.Println("got survey request:", msg)
			byteswg.Wait()
			answer := ""
			for di, d := range opts.Destination {
				answer += fmt.Sprintf("%s:%s:%d\n", hostname, d, totalbytes[di])
			}
			log.Println("sending survey answer:", answer)
			if err = respondersock.Send([]byte(answer)); err != nil {
				log.Panic("Cannot send: %s", err.Error())
			}
		}
		if string(msg) == "EXIT" {
			log.Println("exiting")
			os.Exit(0)
		}
	}
}
