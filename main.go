package main

import(
	"log"
	"net"
	"io"
	"time"
	"net/http"
	"errors"
	"strconv"
	"encoding/json"
	"bytes"
)

const dataBaseAddr = "localhost:6379"
const statServerAddr = "192.168.31.178"
//const statServerAddr = "10.241.88.151"
const statServerPort = ":3030"

var dbConn net.Conn

var stats = AllStats{}

type LinkFollow struct{
	Url string
	Ip string
	Time string
}

type AllStats struct{
	stats []LinkFollow
	count int
}

func dbWriteRead(request string) string{
	ans := make([]byte, 1024)
	io.WriteString(dbConn, request + "\n")
	n, err := dbConn.Read(ans)
	if err != nil {
		log.Panic("Count not read from server")
	}
	log.Println("Wrote to DB command " + request + ".\nGot response: " + string(ans[:n]) + "\n")

	return string(ans[:n])
}

func recover(){
	count, err := strconv.Atoi(dbWriteRead("SPOP"))
	if err != nil {
		return
	}
	stats.count = count
	for i := 0; i < count; i++ {
		stats.stats = append(stats.stats, LinkFollow{})
		newStat := dbWriteRead("AGET " + strconv.Itoa(i))
		buf := new(LinkFollow)
		json.NewDecoder(bytes.NewBuffer([]byte(newStat))).Decode(buf)
		stats.stats[i] = *buf
	}
}

func newStat(w http.ResponseWriter, r *http.Request){
	b := make([]byte, 1024)
	n, err := r.Body.Read(b)											 // read json
	if err != nil {
		log.Panic("could not read new stat")
	}

	newStat  := &LinkFollow{} 											// decode json
	err = json.NewDecoder(r.Body).Decode(newStat) 
	if err != nil {
		log.Println("not a json as request")
		return
	}
	count, err := strconv.Atoi(dbWriteRead("SPOP"))
	if err != nil{
		count = 0
	}
	dbWriteRead("SPUSH " + strconv.Itoa(count + 1))
	stat := string(b[:n])
	dbWriteRead("ASET " + strconv.Itoa(stats.count) + " " + stat)
}

func newRequest (w http.ResponseWriter, r *http.Request){

}

func main(){
	var err error
	dbConn, err = net.Dial("tcp", dataBaseAddr)
	if err != nil{
		log.Panic("Could not connect to DB server. Maybe it is down?")
	}
	dbConn.SetReadDeadline(time.Now().Add(time.Millisecond * 700))
	dbWriteRead("ShortUrlStats")
	recover()
	http.HandleFunc("/", newStat)
	http.HandleFunc("/request", newRequest)

	err = http.ListenAndServe(statServerAddr + statServerPort, nil)
	if errors.Is(err, http.ErrServerClosed) {
		log.Panic("server closed")
	} else {
		log.Panic("error starting server: ", err)
	}
}
