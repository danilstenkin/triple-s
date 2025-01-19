package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"triple-s/handlers"
)

// Функция для проверки и создания директории
func ensureDir(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("Не удалось создать директорию %s: %v", dir, err)
		}
	}
}

func main() {
	// Флаги
	port := flag.Int("port", 8080, "Port number for server")
	dir := flag.String("dir", "./data", "Directory for storing buckets")
	help := flag.Bool("help", false, "Show usage information")
	flag.Parse()

	// Если вызываем --help
	if *help {
		fmt.Println("Simple Storage Service.\n\nUsage:\n    triple-s [-port <N>] [-dir <S>]\n    triple-s --help\n\nOptions:\n- --help     Show this screen.\n- --port N   Port number\n- --dir S    Path to the directory")
		return
	}

	// Устанавливаем корневую директорию в обработчиках
	handlers.BaseDir = *dir

	// Проверяем и создаём директорию
	ensureDir(*dir)

	// Формируем адрес для сервера
	address := fmt.Sprintf(":%d", *port)
	fmt.Printf("Starting server on %s\n", address)

	// Регистрируем обработчики
	http.HandleFunc("/buckets/", handlers.CreateBucketHandler)

	// Запускаем сервер
	err := http.ListenAndServe(address, nil)
	if err != nil {
		log.Fatal(err)
	}
}
