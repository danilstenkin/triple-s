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

// Обработчик для загрузки объектов
func UploadObjectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "PUT" {
		WriteXMLResponse(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Метод не поддерживается")
		return
	}

	// Извлекаем имя бакета и объекта из пути
	pathSegments := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(pathSegments) < 2 {
		WriteXMLResponse(w, http.StatusInternalServerError, "UndefindedRoute", "Неверный путь ")
		return
	}

	bucketName := pathSegments[0]
	objectName := strings.Join(pathSegments[1:], "/")

	// Проверяем существование бакета
	bucketDir := filepath.Join(BaseDir, bucketName)
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		WriteXMLResponse(w, http.StatusNotFound, "BucketIsNotExist", "Бакет не найден")
		return
	}

	// Формируем путь к объекту
	objectPath := filepath.Join(bucketDir, objectName)

	// Создаём директорию для вложенных объектов, если нужно
	if err := os.MkdirAll(filepath.Dir(objectPath), 0o755); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "ErrorDir", "Ошибка создания директорий для объекта")
		return
	}

	// Создаём или перезаписываем объект
	file, err := os.Create(objectPath)
	if err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntCreate", "Ошибка создания объекта")
		return
	}
	defer file.Close()

	// Копируем данные из тела запроса в файл
	written, err := io.Copy(file, r.Body)
	if err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntWrite", "Ошибка записи данных в объект")
		return
	}

	// Определяем MIME-тип объекта
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Обновляем файл object_metadata.csv
	timestamp := time.Now().UTC().Format(time.RFC3339)
	if err := AddObjectToMetadata(bucketName, objectName, contentType, written, timestamp); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "InternalServerError", "Ошибка обновления метаданных объекта")
		return
	}

	// Обновляем статус бакета на Active
	if err := UpdateBucketStatus(bucketName); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "InternalServerError", "Ошибка обновления статуса бакета")
		return
	}

	WriteXMLResponse(w, 201, "Success", "Успешно создан")
	fmt.Fprintf(w, "Объект '%s' успешно создан в бакете '%s'", objectName, bucketName)
}

// Обработчик для удаления объекта
// Обработчик для удаления объекта
func DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
		WriteXMLResponse(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Метод не поддерживается")
		return
	}

	// Извлекаем имя бакета и объекта из пути
	pathSegments := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(pathSegments) < 2 {
		WriteXMLResponse(w, http.StatusBadRequest, "RouteNotFound", "Неверный путь. Ожидалось /{bucketName}/{objectName}")
		WriteXMLResponse(w, http.StatusBadRequest, "", "")
		return
	}

	bucketName := pathSegments[0]
	objectName := strings.Join(pathSegments[1:], "/")

	// Проверяем наличие записи в object_metadata.csv
	exists, err := isObjectInMetadata(bucketName, objectName)
	if err != nil {
		WriteXMLResponse(w, http.StatusMethodNotAllowed, "CouldntReadMetadata", "Ошибка чтения файла метаданных объектов")
		return
	}
	if !exists {
		WriteXMLResponse(w, http.StatusMethodNotAllowed, "CouldntReadFile", "Объект не найден в метаданных, удаление запрещено")
		return
	}

	// Формируем путь к объекту
	objectPath := filepath.Join(BaseDir, bucketName, objectName)

	// Проверяем существование объекта
	if _, err := os.Stat(objectPath); os.IsNotExist(err) {
		WriteXMLResponse(w, http.StatusNotFound, "Object", "")
		return
	}

	// Удаляем объект
	if err := os.Remove(objectPath); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntDelete", "Ошибка удаления объекта")
		return
	}

	// Удаляем запись об объекте из метаданных
	if err := DeleteObjectFromMetadata(bucketName, objectName); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntDelete", "Ошибка удаления записи из файла метаданных")
		return
	}

	WriteXMLResponse(w, http.StatusNoContent, "Deleted", "Deleted")
}

func getFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// Обработчик для получения объекта
func GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		WriteXMLResponse(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Метод не поддерживается")
		return
	}

	// Извлекаем имя бакета и объекта из пути
	pathSegments := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(pathSegments) < 2 {
		WriteXMLResponse(w, http.StatusBadRequest, "RouteError", "Неверный путь. Ожидалось /{bucketName}/{objectName}")
		return
	}

	bucketName := pathSegments[0]
	objectName := strings.Join(pathSegments[1:], "/")

	// Проверяем существование бакета
	bucketDir := filepath.Join(BaseDir, bucketName)
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		WriteXMLResponse(w, http.StatusNotFound, "BucketNotFound", "Бакет не найден")
		return
	}

	// Формируем путь к объекту
	objectPath := filepath.Join(bucketDir, objectName)

	// Проверяем существование объекта
	file, err := os.Open(objectPath)
	if os.IsNotExist(err) {
		WriteXMLResponse(w, http.StatusNotFound, "NotFound", "Бакет не найден")
		return
	} else if err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntOpen", "Ошибка открытия")
		return
	}
	defer file.Close()

	// Определяем MIME-тип объекта
	buffer := make([]byte, 512)
	n, _ := file.Read(buffer)
	contentType := http.DetectContentType(buffer[:n])
	file.Seek(0, 0) // Возвращаем указатель на начало файла

	// Устанавливаем корректные заголовки ответа
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", getFileSize(objectPath)))

	// Передаём содержимое файла клиенту
	if _, err := io.Copy(w, file); err != nil {
		WriteXMLResponse(w, http.StatusInternalServerError, "CouldntShare", "Ошибка передачи объекта")
		return
	}
}
