// Copyright 2024 Jacqurs Supcik. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

// Author : Jacques Supcik <jacques#supcik.net>
// Created: 2024-10-25

// This program reads a log file from stdin and replays it as MQTT messages.
// The log file should be in the following format:
//   HH:MM:SS.uuu TOPIC MESSAGE
// It can be preceded by some additional date information.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	var broker = flag.String("host", "localhost", "MQTT Broker Host")
	var port = flag.Int("port", 1883, "MQTT Broker Port")

	flag.Parse()
	fmt.Println("Broker Host:", *broker)

	mqttClient := mqtt.NewClient(mqtt.NewClientOptions().AddBroker("tcp://" + *broker + ":" + strconv.Itoa(*port)))
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	scanner := bufio.NewScanner(os.Stdin)
	re := regexp.MustCompile(`(\d{2}):(\d{2}):(\d{2})\.(\d+) ([^ ]+) (.*)`)
	firstLine := true
	var t0 time.Duration
	var now time.Time
	for {
		scanner.Scan()
		line := scanner.Text()
		if len(line) == 0 {
			break
		}
		record := re.FindStringSubmatch(line)

		h, _ := strconv.Atoi(record[1])
		m, _ := strconv.Atoi(record[2])
		s, _ := strconv.Atoi(record[3])
		us, _ := strconv.Atoi(record[4])

		ts := time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second + time.Duration(us)*time.Microsecond

		if firstLine {
			t0 = ts
			now = time.Now()
			firstLine = false
		} else {
			time.Sleep(time.Until(now.Add(ts - t0)))
		}

		fmt.Printf("Publishing '%s' on '%s'\n", record[6], record[5])
		token := mqttClient.Publish(record[5], 0, false, record[6])
		token.Wait()
	}
}
