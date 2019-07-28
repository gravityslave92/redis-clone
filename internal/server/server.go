package server

import (
	"encoding/csv"
	"fmt"
	"net"
	"redis_like_in_memory_db/internal/global_cache"
	"strings"
)

type Server struct {
	Port             string
	PasswordRequired bool
	Password         string
	cache            *global_cache.GlobalCache
}

func NewServer(port string, passwordRequired bool, password string) *Server {
	return &Server{
		Port:             port,
		PasswordRequired: passwordRequired,
		Password:         password,
		cache:            global_cache.NewCache(),
	}
}

func (s *Server) Run() {
	address, err := net.ResolveTCPAddr("tcp", s.Port)
	if err != nil {
		return
	}

	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		fmt.Println(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		conn.Write([]byte("SSuccessful connection\n"))

		go handleConn(conn, s)
	}
}

func handleConn(conn net.Conn, server *Server) {
	if server.PasswordRequired {
		authorizeConnection(conn, server.Password)
	}
	
	// accept inputs
	parseRequest(conn, server)
}

func authorizeConnection(conn net.Conn, serverPassword string) {
	conn.Write([]byte("Authorize to proceed\n"))

	var buf [512]byte
	for {
		n, err := conn.Read(buf[0:])
		if err != nil {
			continue
		}

		password := strings.TrimSpace(string(buf[0:n]))
		if password == serverPassword {
			conn.Write([]byte("You have authorized  successfully\n"))
			break
		}

		conn.Write([]byte("Incorrect attempt of authorization\n"))
	}
}

func parseRequest(conn net.Conn, server *Server) {
	var buf [512]byte
	for {
		n, err := conn.Read(buf[0:])
		if err != nil {
			return
		}

		args := parseMessage(string(buf[0:n]))
		if args == nil {
			conn.Write([]byte("Please, send non-empty message"))
			continue
		}

		response := server.cache.ProcessCommand(args)
		conn.Write([]byte(response))
	}
}

func parseMessage(msg string) []string {
	result := make([]string, 0)
	csvReader := csv.NewReader(strings.NewReader(msg))
	csvReader.Comma = ' ' // space
	fields, err := csvReader.Read()
	if err != nil {
		return nil
	}

	for _, field := range fields {
		if field == "" {
			continue
		}

		result = append(result, field)
	}

	return result
}
