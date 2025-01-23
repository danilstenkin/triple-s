package handlers

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func UploadObjectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "PUT" {
		WriteXMLResponse(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Метод не поддерживается")
		return
	}

	pathSegments := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(pathSegments) < 2 {
		WriteXMLResponse(w, http.StatusInternalServerError, "UndefindedRoute", "Неверный путь ")
		return
	}

	if r.ContentLength == 0 {
		WriteXMLResponse(w, http.StatusBadRequest, "EmptyObject", "Объект не может быть пустым")
		return
	}

	bucketName := pathSegments[0]
	objectName := strings.Join(pathSegments[1:], "/")

	if strings.Contains(bucketName, ".") {
		WriteXMLResponse(w, http.StatusUnsupportedMediaType, "BucketKeyShouldntHasType", "Bucket не должен содержать рассширения")
		return
	}
	if strings.Contains(objectName, ".") {
		WriteXMLResponse(w, http.StatusUnsupportedMediaType, "ObjectKeyShouldntHasType", "ObjectKey не должен содержать рассширения")
		return
	}

	bucketDir := filepath.Join(BaseDir, bucketName)
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		WriteXMLResponse(w, http.StatusNotFound, "BucketIsNotExist", "Бакет не найден")
		return
	}

	objectPath := filepath.Join(bucketDir, objectName)

	if err := os.MkdirAll(filepath.Dir(objectPath), 0o755); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "ErrorDir", "Ошибка создания директорий для объекта")
		return
	}

	file, err := os.Create(objectPath)
	if err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntCreate", "Ошибка создания объекта")
		return
	}
	defer file.Close()

	written, err := io.Copy(file, r.Body)
	if err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntWrite", "Ошибка записи данных в объект")
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	timestamp := time.Now().UTC().Format(time.RFC3339)
	if err := AddObjectToMetadata(bucketName, objectName, contentType, written, timestamp); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "InternalServerError", "Ошибка обновления метаданных объекта")
		return
	}

	if err := UpdateBucketStatus(bucketName); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "InternalServerError", "Ошибка обновления статуса бакета")
		return
	}

	WriteXMLResponse(w, 201, "Success", "Успешно создан")
	fmt.Fprintf(w, "Объект '%s' успешно создан в бакете '%s'", objectName, bucketName)
}

func DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
		WriteXMLResponse(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Метод не поддерживается")
		return
	}

	pathSegments := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(pathSegments) < 2 {
		WriteXMLResponse(w, http.StatusBadRequest, "RouteNotFound", "Неверный путь. Ожидалось /{bucketName}/{objectName}")
		return
	}

	bucketName := pathSegments[0]
	objectName := strings.Join(pathSegments[1:], "/")

	if objectName == "objects.csv" {
		WriteXMLResponse(w, http.StatusForbidden, "Forbidden", "Удаление файла objects.csv запрещено")
		return
	}

	exists, err := isObjectInMetadata(bucketName, objectName)
	if err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntReadMetadata", "Ошибка чтения файла метаданных объектов")
		return
	}
	if !exists {
		WriteXMLResponse(w, http.StatusNotFound, "ObjectNotFound", "Объект не найден в метаданных")
		return
	}

	objectPath := filepath.Join(BaseDir, bucketName, objectName)

	if _, err := os.Stat(objectPath); os.IsNotExist(err) {
		WriteXMLResponse(w, http.StatusNotFound, "ObjectNotFound", "Объект не найден")
		return
	}

	if err := os.Remove(objectPath); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntDelete", "Ошибка удаления объекта")
		return
	}

	if err := DeleteObjectFromMetadata(bucketName, objectName); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntDeleteMetadata", "Ошибка удаления записи из файла метаданных")
		return
	}

	if err := UpdateBucketStatus(bucketName); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "InternalServerError", "Ошибка обновления статуса бакета")
		return
	}

	WriteXMLResponse(w, http.StatusNoContent, "Deleted", "Объект успешно удалён")
}

func getFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		WriteXMLResponse(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Метод не поддерживается")
		return
	}

	pathSegments := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(pathSegments) < 2 {
		WriteXMLResponse(w, http.StatusBadRequest, "RouteError", "Неверный путь. Ожидалось /{bucketName}/{objectName}")
		return
	}

	bucketName := pathSegments[0]
	objectName := strings.Join(pathSegments[1:], "/")

	bucketDir := filepath.Join(BaseDir, bucketName)
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		WriteXMLResponse(w, http.StatusNotFound, "BucketNotFound", "Бакет не найден")
		return
	}

	objectPath := filepath.Join(bucketDir, objectName)

	file, err := os.Open(objectPath)
	if os.IsNotExist(err) {
		WriteXMLResponse(w, http.StatusNotFound, "NotFound", "Бакет не найден")
		return
	} else if err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntOpen", "Ошибка открытия")
		return
	}
	defer file.Close()

	buffer := make([]byte, 512)
	n, _ := file.Read(buffer)
	contentType := http.DetectContentType(buffer[:n])
	file.Seek(0, 0)

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", getFileSize(objectPath)))

	if _, err := io.Copy(w, file); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntShare", "Ошибка передачи объекта")
		return
	}
}
