package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// Message is the JSON shape clients send and receive.
type Message struct {
	Username string `json:"username"`
	Text     string `json:"text"`
}

// clients stores every connected WebSocket client.
// The bool value is not important; using a map makes add/remove easy.
var clients = make(map[*websocket.Conn]bool)

// clientsMu protects the clients map because many clients can connect at once.
var clientsMu sync.Mutex

// upgrader turns a normal HTTP request into a WebSocket connection.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// Allow LAN clients from different hosts to connect during development.
		return true
	},
}

func main() {
	http.HandleFunc("/", handleHome)
	http.HandleFunc("/ws", handleWebSocket)

	log.Println("l-chat backend listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleHome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("l-chat backend is running"))
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("websocket upgrade failed:", err)
		return
	}

	// Add this connection to the in-memory client list.
	clientsMu.Lock()
	clients[conn] = true
	clientsMu.Unlock()

	log.Println("client connected")

	// Remove the connection when this function exits.
	defer func() {
		clientsMu.Lock()
		delete(clients, conn)
		clientsMu.Unlock()

		conn.Close()
		log.Println("client disconnected")
	}()

	for {
		var message Message

		// Read one JSON message from this client.
		err := conn.ReadJSON(&message)
		if err != nil {
			log.Println("read failed:", err)
			break
		}

		broadcast(message)
	}
}

func broadcast(message Message) {
	data, err := json.Marshal(message)
	if err != nil {
		log.Println("message marshal failed:", err)
		return
	}

	clientsMu.Lock()
	defer clientsMu.Unlock()

	// Send the message to every connected client, including the sender.
	for client := range clients {
		err := client.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			log.Println("write failed:", err)
			client.Close()
			delete(clients, client)
		}
	}
}
