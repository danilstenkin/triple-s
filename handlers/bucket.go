package handlers

import (
	"encoding/csv"
	"encoding/xml"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var BaseDir string

func isValidBucketName(bucketName string) bool {
	if len(bucketName) < 3 || len(bucketName) > 63 {
		return false
	}

	if strings.Contains(bucketName, ".") {
		return false
	}

	matched, _ := regexp.MatchString(`^[a-z0-9][a-z0-9.-]*[a-z0-9]$`, bucketName)
	if !matched {
		return false
	}

	if strings.Contains(bucketName, "..") {
		return false
	}

	if net.ParseIP(bucketName) != nil {
		return false
	}

	disallowedPrefixes := []string{"xn--", "sthree-", "sthree-configurator", "amzn-s3-demo-"}
	for _, prefix := range disallowedPrefixes {
		if strings.HasPrefix(bucketName, prefix) {
			return false
		}
	}

	disallowedSuffixes := []string{"-s3alias", "--ol-s3", ".mrap", "--x-s3"}
	for _, suffix := range disallowedSuffixes {
		if strings.HasSuffix(bucketName, suffix) {
			return false
		}
	}

	return true
}

func CreateBucketHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "PUT" {
		WriteXMLResponse(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Метод не поддерживается")
		return
	}

	bucketName := strings.TrimPrefix(r.URL.Path, "/")
	if bucketName == "" {
		WriteXMLResponse(w, http.StatusBadRequest, "BucketnameIsNotExits", "Название бакета не указано")
		return
	}

	if !isValidBucketName(bucketName) {
		WriteXMLResponse(w, http.StatusBadRequest, "UnvaliabaleName", "Недопустимое имя бакета")
		return
	}

	bucketDir := filepath.Join(BaseDir, bucketName)

	if _, err := os.Stat(bucketDir); !os.IsNotExist(err) {
		WriteXMLResponse(w, http.StatusConflict, "Already exists", "Бакет уже существует")
		return
	}

	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntCreate", "Ошибка создания директории бакета")
		return
	}

	objectMetadataPath := filepath.Join(bucketDir, "objects.csv")
	file, err := os.Create(objectMetadataPath)
	if err != nil {
		WriteXMLResponse(w, http.StatusBadRequest, "CouldntCreateFile", "Ошибка создания xml файла")
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{"ObjectName", "Size", "ContentType", "LastModified"}); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntWriteHeader", "Ошибка записи заголовков в objects.csv")
		return
	}

	timestamp := time.Now().UTC().Format(time.RFC3339)
	if err := AddBucketToMetadata(bucketName, timestamp); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntUpdateMetada", "Ошибка добавления метаданных")
		return
	}

	WriteXMLResponse(w, http.StatusCreated, "Success", "Bucket успешно создан")
}

func DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
		WriteXMLResponse(w, http.StatusMethodNotAllowed, "Not Allowed", "Метод не поддерживается")
		return
	}

	bucketName := strings.TrimPrefix(r.URL.Path, "/")
	if bucketName == "" {
		WriteXMLResponse(w, http.StatusBadRequest, "Backet name not exists", "Название бакета не указано")
		return
	}

	if bucketName == "buckets.csv" {
		WriteXMLResponse(w, http.StatusBadRequest, "Forbidden", "Удаление файла buckets.csv запрещено")
		return
	}

	exists, err := isBucketInMetadata(bucketName)
	if err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "Error of metadata", "Ошибка чтения файла метаданных бакетов")
		return
	}
	if !exists {
		WriteXMLResponse(w, http.StatusNotFound, "Couldn't DELETE", "Бакет не найден в метаданных, удаление запрещено")
		return
	}

	bucketDir := filepath.Join(BaseDir, bucketName)

	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		WriteXMLResponse(w, http.StatusNotFound, "Not Found", "Бакет не найден")
		return
	}

	entries, err := os.ReadDir(bucketDir)
	if err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "Error of reading file", "Ошибка чтения содержимого бакета")
		return
	}

	if len(entries) > 1 || (len(entries) == 1 && entries[0].Name() != "objects.csv") {
		WriteXMLResponse(w, http.StatusConflict, "Buscket not empty", "Бакет не пуст, удаление запрещено")
		return
	}

	if err := os.RemoveAll(bucketDir); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "Error of delete", "Ошибка удаления директории бакета")
		return
	}

	if err := RemoveBucketFromMetadata(bucketName); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "Couldn't DELETE", "Ошибка удаления записи из файла метаданных")
		return
	}

	WriteXMLResponse(w, 204, "Successful", "Бакет успешно создан !")
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
