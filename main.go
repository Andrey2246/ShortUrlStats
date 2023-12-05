package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
)

const dataBaseAddr = "localhost:6379"
const statServerAddr = "192.168.31.178"

// const statServerAddr = "10.241.88.151"
const statServerPort = ":3030"

var dbConn net.Conn

var stats = AllStats{}

type LinkFollow struct {
	Url  string
	Ip   string
	Time string
}

type AllStats struct {
	stats []LinkFollow
	count int
}

type Request struct {
	Dimensions [3]string
}

type ResponseRow struct {
	Id           int
	Pid          int
	Url          string
	SourseIp     string
	TimeInterval string
	Count        int
}
type Response []ResponseRow

func dbWriteRead(request string) string {
	ans := make([]byte, 1024)
	io.WriteString(dbConn, request+"\n")
	n, err := dbConn.Read(ans)
	if err != nil {
		log.Panic("Count not read from server" + request)
	}
	log.Println("Wrote to DB command " + request + ".\nGot response: " + string(ans[:n]) + "\n")

	return string(ans[:n])
}

func (l LinkFollow) GetField(r string) string {
	switch r {
	case "Url":
		return l.Url
	case "Ip":
		return l.Ip
	case "Time":
		return l.Time
	}
	return ""
}

func recover() {
	log.Println("Recovering")
	dbResponse := dbWriteRead("SPOP")
	count, err := strconv.Atoi(dbResponse[:len(dbResponse)-1])
	if err != nil {
		log.Println(err)
		log.Println("Nothing to recover")
		return
	}
	stats.count = count
	for i := 0; i < count; i++ {
		log.Println("Recovering " + strconv.Itoa(i))
		stats.stats = append(stats.stats, LinkFollow{})
		stats.count += 1
		newStat := dbWriteRead("AGET " + strconv.Itoa(i))
		buf := new(LinkFollow)
		json.NewDecoder(bytes.NewBuffer([]byte(newStat))).Decode(buf)
		stats.stats[i] = *buf
	}
	dbWriteRead("SPUSH " + strconv.Itoa(count))
}

func newStat(w http.ResponseWriter, r *http.Request) {
	newStat := &LinkFollow{} // decode json
	err := json.NewDecoder(r.Body).Decode(&newStat)
	if err != nil {
		log.Println("not a json as request")
		return
	}
	count, err := strconv.Atoi(dbWriteRead("SPOP"))
	if err != nil {
		count = 0
	}
	stats.stats = append(stats.stats, *newStat)
	stats.count += 1
	dbWriteRead("SPUSH " + strconv.Itoa(count+1))
	stat, _ := json.Marshal(newStat)
	dbWriteRead("ASET " + strconv.Itoa(stats.count) + " " + string(stat))
}

func newRequest(w http.ResponseWriter, r *http.Request) {
	req := &Request{} // decode json
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		log.Println("not a json as request")
		return
	}
	for i := 0; i < stats.count; i++ {
		f1i := stats.stats[i].GetField(req.Dimensions[0])
		for j := 0; j < stats.count; j++ {
			f1j := stats.stats[j].GetField(req.Dimensions[0])
			if f1i == f1j {
				f2j := stats.stats[j].GetField(req.Dimensions[1])
				for k := 0; k < stats.count; k++ {
					f3 := stats.stats[k].GetField(req.Dimensions[2])

				}
			}

		}
	}
}

func main() {
	var err error
	dbConn, err = net.Dial("tcp", dataBaseAddr)
	if err != nil {
		log.Panic("Could not connect to DB server. Maybe it is down?")
	}
	dbWriteRead("ShortUrlStats")
	recover()
	http.HandleFunc("/", newStat)
	http.HandleFunc("/request", newRequest)

	err = http.ListenAndServe(statServerAddr+statServerPort, nil)
	if errors.Is(err, http.ErrServerClosed) {
		log.Panic("server closed")
	} else {
		log.Panic("error starting server: ", err)
	}
}
