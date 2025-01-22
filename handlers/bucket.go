package handlers

import (
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var BaseDir string

func isValidBucketName(bucketName string) bool {
	matched, _ := regexp.MatchString(`^[a-z0-9]([a-z0-9.-]{1,61}[a-z0-9])?$`, bucketName)
	return matched && !strings.Contains(bucketName, "..")
}

func CreateBucketHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "PUT" {
		WriteXMLResponse(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Метод не поддерживается")
		return
	}

	// Извлекаем имя бакета из пути
	bucketName := strings.TrimPrefix(r.URL.Path, "/")
	if bucketName == "" {
		http.Error(w, "Название бакета не указано", http.StatusBadRequest)
		return
	}

	// Проверяем валидность имени бакета
	if !isValidBucketName(bucketName) {
		http.Error(w, "Недопустимое имя бакета", http.StatusBadRequest)
		return
	}

	// Формируем путь к бакету
	bucketDir := filepath.Join(BaseDir, bucketName)

	// Проверяем, существует ли уже такая директория
	if _, err := os.Stat(bucketDir); !os.IsNotExist(err) {
		WriteXMLResponse(w, http.StatusConflict, "Already exists", "Бакет уже существует")
		return
	}

	// Создаём директорию для бакета
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		http.Error(w, "Ошибка создания директории бакета", http.StatusInternalServerError)
		return
	}

	// Создаём файл object_metadata.csv внутри бакета
	objectMetadataPath := filepath.Join(bucketDir, "object_metadata.csv")
	file, err := os.Create(objectMetadataPath)
	if err != nil {
		http.Error(w, "Ошибка создания файла object_metadata.csv", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// Записываем заголовки в object_metadata.csv
	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{"ObjectName", "Size", "ContentType", "LastModified"}); err != nil {
		http.Error(w, "Ошибка записи заголовков в object_metadata.csv", http.StatusInternalServerError)
		return
	}

	// Добавляем метаданные бакета
	timestamp := time.Now().UTC().Format(time.RFC3339)
	if err := AddBucketToMetadata(bucketName, timestamp); err != nil {
		http.Error(w, "Ошибка добавления метаданных", http.StatusInternalServerError)
		return
	}

	// Успешный ответ
	w.WriteHeader(http.StatusCreated)
	WriteXMLResponse(w, http.StatusCreated, "Success", "'%s' Успешно создан")
}

// Обработчик для удаления бакета
func DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	// Извлекаем имя бакета из пути
	bucketName := strings.TrimPrefix(r.URL.Path, "/")
	if bucketName == "" {
		http.Error(w, "Название бакета не указано", http.StatusBadRequest)
		return
	}

	// Проверяем наличие записи в buckets_metadata.csv
	exists, err := isBucketInMetadata(bucketName)
	if err != nil {
		http.Error(w, "Ошибка чтения файла метаданных бакетов", http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, "Бакет не найден в метаданных, удаление запрещено", http.StatusNotFound)
		return
	}

	// Формируем путь к директории бакета
	bucketDir := filepath.Join(BaseDir, bucketName)

	// Проверяем существование директории
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		http.Error(w, "Бакет не найден", http.StatusNotFound)
		return
	}

	// Проверяем, пуст ли бакет
	entries, err := os.ReadDir(bucketDir)
	if err != nil {
		http.Error(w, "Ошибка чтения содержимого бакета", http.StatusInternalServerError)
		return
	}

	if len(entries) > 1 || (len(entries) == 1 && entries[0].Name() != "object_metadata.csv") {
		http.Error(w, "Бакет не пуст, удаление запрещено", http.StatusConflict)
		return
	}

	// Удаляем директорию бакета
	if err := os.RemoveAll(bucketDir); err != nil {
		http.Error(w, "Ошибка удаления директории бакета", http.StatusInternalServerError)
		return
	}

	// Удаляем запись из метаданных
	if err := RemoveBucketFromMetadata(bucketName); err != nil {
		http.Error(w, "Ошибка удаления записи из файла метаданных", http.StatusInternalServerError)
		return
	}

	// Возвращаем успешный ответ
	w.WriteHeader(http.StatusNoContent)
	fmt.Fprintf(w, "Бакет '%s' успешно удалён", bucketName)
}

type Bucket struct {
	Name string `xml:"Name"`
}

type ListBucketsResponse struct {
	XMLName xml.Name `xml:"ListBuckets"`
	Buckets []Bucket `xml:"Bucket"`
}

func ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		WriteXMLResponse(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Метод не поддерживается")
		return
	}

	entries, err := os.ReadDir(BaseDir)
	if err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "InternalError", "Ошибка чтения директории")
		return
	}

	var buckets []Bucket
	for _, entry := range entries {
		if entry.IsDir() {
			buckets = append(buckets, Bucket{Name: entry.Name()})
		}
	}

	response := ListBucketsResponse{Buckets: buckets}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	if err := xml.NewEncoder(w).Encode(response); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "InternalError", "Ошибка формирования ответа")
	}
}
