package main

import (
	"log"
	"net/http"
	"os"
)

func Wtite(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Hellow World"))
		return
	} else {
		w.Write([]byte("Sorry, only method GeT"))
	}
}

func CreateBucket(w http.ResponseWriter, r *http.Request) {
	if _, err := os.Stat("/data/"); os.IsNotExist(err) {
		if err := os.MkdirAll("/data/", 0755); err != nil {
			log.Fatalf("Failed to create directory: %v\n", err)
		}
	}
}

func main() {
	http.HandleFunc("/", Wtite)
	http.HandleFunc("/put", CreateBucket)

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}
