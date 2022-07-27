package main

import (
	"log"
	"net"
	"os"
	"strconv"
	"sync"

	"go.nanomsg.org/mangos/v3"
	//"go.nanomsg.org/mangos/v3/protocol/pub"
	"go.nanomsg.org/mangos/v3/protocol/sub"

	// register transports
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

func receiver() {
	var sock mangos.Socket
	var err error
	var msg []byte

	var stopflag bool
	var stopmutex sync.RWMutex

	stopmutex.Lock()
	stopflag = false
	stopmutex.Unlock()

	hostname, _ := os.Hostname()

	// create listening sockets

	for i := 0; i < opts.Sockets; i++ {
		go func(port int) {
			server, err := net.Listen("tcp" /* hostname+ */, ":"+strconv.Itoa(port))
			if err != nil {
				log.Println("Error listening:", err.Error())
				os.Exit(1)
			}
			defer server.Close()
			log.Println("Listening on " + hostname + ":" + strconv.Itoa(port))
			log.Println("Waiting for client...")
			for {
				connection, err := server.Accept()
				if err != nil {
					log.Println("Error accepting: ", err.Error())
					os.Exit(1)
				}
				/*
					err = connection.SetReadBuffer(1024 * 1024)
					if err != nil {
						log.Println("could not set socket buffer size to 1MiB")
					}
				*/
				// work here
				buffer := make([]byte, 1024*1024)
				//mlen, err := connection.Read(buffer)
				for {
					_, _ = connection.Read(buffer)

					stopmutex.RLock()
					if stopflag {
						stopmutex.RUnlock()
						break
					}
					stopmutex.RUnlock()
				}

			}
		}(i + opts.Basesocket)
	}

	//subscribe

	if sock, err = sub.NewSocket(); err != nil {
		log.Panic("can't get new sub socket: %s", err.Error())
	}
	if err = sock.Dial(cmdurl); err != nil {
		log.Panic("can't dial on sub socket: %s", err.Error())
	}
	// Empty byte array effectively subscribes to everything
	err = sock.SetOption(mangos.OptionSubscribe, []byte(""))
	if err != nil {
		log.Panic("cannot subscribe: %s", err.Error())
	}
	for {
		if msg, err = sock.Recv(); err != nil {
			log.Panic("Cannot recv: %s", err.Error())
		}
		if string(msg) == "STOP" {
			log.Println("stopping benchmark")
			stopmutex.Lock()
			stopflag = true
			stopmutex.Unlock()
		}
		if string(msg) == "EXIT" {
			log.Println("exiting")
			os.Exit(0)
		}
	}
}
