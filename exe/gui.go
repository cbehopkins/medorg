package main

import (
	"log"
	"os"

	"github.com/icza/gowut/gwu"

	"net"

	"github.com/cbehopkins/medorg"
)

// Get preferred outbound ip of this machine
func GetOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr, _, _ := net.SplitHostPort(conn.LocalAddr().String())
	if err != nil {
		localAddr = ""
	}

	return localAddr
}
func serverString(local bool) string {
	host_string := "localhost"
	port := "8081"
	if !local {
		ip := GetOutboundIP()
		if ip != "" {
			host_string = ip
		}
	}
	return host_string + ":" + port
}
func main() {
	// Keep it from advertising to external network
	// Good for windows firewall
	localServer := true
	// Create and start a GUI server (omitting error check)
	server := gwu.NewServer("medorg", serverString(localServer))
	server.SetLogger(log.New(os.Stdout, "", log.Lshortfile))
	server.SetText("Media Organiser")

	// Create and build a window

	fileWin := medorg.FileWin(".")
	//

	server.AddWin(fileWin)

	server.Start("file") // Also opens windows list in browser
	//server.Start("") // Also opens windows list in browser
}
