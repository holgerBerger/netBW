package main

import (
	"log"
	"os/exec"
	"strings"
	"time"
)

// spawnProcess starts workers, retrying 10 times/max 20 seconds
// waiting 1 second between retries
func spawnProcess(c string, option string, path string, url string, sureyurl string, basesocket string) {
	var count int
	count = 0
	var killed bool

	// compare sha224 of local and remote collector, and skip installation if same,
	// install if anything goes wrong (like not existing)
	out, lerr := exec.Command("sha224sum", path).CombinedOutput()
	localsha := strings.Fields(string(out))[0]
	out, rerr := exec.Command("ssh", c, "sha224sum", path).CombinedOutput()
	remotesha := strings.Fields(string(out))[0]

	if (localsha != remotesha) || (lerr != nil) || (rerr != nil) {
		// kill runnning processes in case there is one to avoid busy binary error for scp
		_, err := exec.Command("ssh", c, "killall", "-e", path).CombinedOutput()
		killed = true
		log.Println("installing worker on " + c + " in " + path)
		out, err = exec.Command("scp", path, c+":"+path).CombinedOutput()
		if err != nil {
			log.Println("error: unexpected end on " + c)
			log.Println(string(out))
		}
		log.Println("installed worker on " + c)
	} else {
		log.Println("same hash, skipped worker installation on", c)
		killed = false
	}

	t1 := time.Now()
	for {
		// kill runnning processes in case there is one
		if !killed {
			exec.Command("ssh", c, "killall", "-e", path).CombinedOutput()
			killed = true
		}

		log.Println("starting worker on " + c)
		count++
		out, err := exec.Command("ssh", c, path, option, "--commander", cmdurl, "--survey", surveyurl, "--basesocket", basesocket).CombinedOutput()
		if err != nil {
			log.Println("error: unexpected end on "+c, " : ", err)
			log.Println(string(out))
		}
		t2 := time.Now()
		// if we restart 10 times within 20 seconds, something is strange,
		// we bail out that one, may be the config is bad, and it will
		// never work, so do not waste cycles
		if count >= 10 && t2.Sub(t1).Seconds() < 20 {
			log.Println("error: could not start for 10 times, giving up on host " + c)
			break
		}
		time.Sleep(1 * time.Second)
	}
}
