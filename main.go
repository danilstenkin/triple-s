package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"triple-s/handlers"
)

func ensureDir(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatalf("Неудалось создать директорию %s: %v", dir, err)
		}
	}
}

func main() {
	port := flag.Int("port", 8080, "Port number for server")
	dir := flag.String("dir", "data", "Directory for storing buckets")
	flag.Parse()

	if !handlers.IsValidDir(*dir) {
		log.Fatalf("Недопустимое имя директории: %s", *dir)
	}

	ensureDir(*dir)
	handlers.BaseDir = *dir

	if err := handlers.InitializeMetadataFile(*dir); err != nil {
		log.Fatalf("Ошибка инициализации файла метаданных: %v", err)
	}

	address := fmt.Sprintf(":%d", *port)
	fmt.Printf("Сервер запущен на %s\n", address)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		pathSegments := handlers.ParseURLPath(r.URL.Path)
		if len(pathSegments) == 0 {
			switch r.Method {
			case "GET":
				handlers.ListBucketsHandler(w, r)
			default:
				handlers.WriteXMLResponse(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Метод не поддерживается")
			}
		} else if len(pathSegments) == 1 {
			switch r.Method {
			case "PUT":
				handlers.CreateBucketHandler(w, r)
			case "DELETE":
				handlers.DeleteBucketHandler(w, r)
			default:
				handlers.WriteXMLResponse(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Метод не поддерживается")
			}
		} else if len(pathSegments) >= 2 {
			switch r.Method {
			case "PUT":
				handlers.UploadObjectHandler(w, r)
			case "DELETE":
				handlers.DeleteObjectHandler(w, r)
			case "GET":
				handlers.GetObjectHandler(w, r)
			default:
				handlers.WriteXMLResponse(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Метод не поддерживается")
			}
		} else {
			handlers.WriteXMLResponse(w, http.StatusBadRequest, "InvalidPath", "Неверный путь")
		}
	})

	log.Fatal(http.ListenAndServe(address, nil))
}
