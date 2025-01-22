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
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	// Извлекаем имя бакета и объекта из пути
	pathSegments := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(pathSegments) < 2 {
		http.Error(w, "Неверный путь. Ожидалось /{bucketName}/{objectName}", http.StatusBadRequest)
		return
	}

	bucketName := pathSegments[0]
	objectName := strings.Join(pathSegments[1:], "/")

	// Проверяем существование бакета
	bucketDir := filepath.Join(BaseDir, bucketName)
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		http.Error(w, "Бакет не найден", http.StatusNotFound)
		return
	}

	// Формируем путь к объекту
	objectPath := filepath.Join(bucketDir, objectName)

	// Создаём директорию для вложенных объектов, если нужно
	if err := os.MkdirAll(filepath.Dir(objectPath), 0o755); err != nil {
		http.Error(w, "Ошибка создания директорий для объекта", http.StatusInternalServerError)
		return
	}

	// Создаём или перезаписываем объект
	file, err := os.Create(objectPath)
	if err != nil {
		http.Error(w, "Ошибка создания объекта", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// Копируем данные из тела запроса в файл
	written, err := io.Copy(file, r.Body)
	if err != nil {
		http.Error(w, "Ошибка записи данных в объект", http.StatusInternalServerError)
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
		http.Error(w, "Ошибка обновления метаданных объекта", http.StatusInternalServerError)
		return
	}

	// Обновляем статус бакета на Active
	if err := UpdateBucketStatus(bucketName); err != nil {
		http.Error(w, "Ошибка обновления статуса бакета", http.StatusInternalServerError)
		return
	}

	// Возвращаем успешный ответ
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Объект '%s' успешно создан в бакете '%s'", objectName, bucketName)
}

// Обработчик для удаления объекта
func DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	// Извлекаем имя бакета и объекта из пути
	pathSegments := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(pathSegments) < 2 {
		http.Error(w, "Неверный путь. Ожидалось /{bucketName}/{objectName}", http.StatusBadRequest)
		return
	}

	bucketName := pathSegments[0]
	objectName := strings.Join(pathSegments[1:], "/")

	// Проверяем существование бакета
	bucketDir := filepath.Join(BaseDir, bucketName)
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		http.Error(w, "Бакет не найден", http.StatusNotFound)
		return
	}

	// Формируем путь к объекту
	objectPath := filepath.Join(bucketDir, objectName)

	// Проверяем существование объекта
	if _, err := os.Stat(objectPath); os.IsNotExist(err) {
		http.Error(w, "Объект не найден", http.StatusNotFound)
		return
	}

	// Удаляем объект
	if err := os.Remove(objectPath); err != nil {
		http.Error(w, "Ошибка удаления объекта", http.StatusInternalServerError)
		return
	}

	// Удаляем запись об объекте из метаданных
	if err := DeleteObjectFromMetadata(bucketName, objectName); err != nil {
		http.Error(w, "Ошибка удаления записи из object_metadata.csv", http.StatusInternalServerError)
		return
	}

	// Проверяем, пуст ли бакет (если кроме object_metadata.csv ничего нет)
	isEmpty, err := isBucketEmpty(bucketDir)
	if err != nil {
		http.Error(w, "Ошибка проверки содержимого бакета", http.StatusInternalServerError)
		return
	}

	// Если бакет пуст, обновляем статус на Inactive
	if isEmpty {
		if err := UpdateBucketStatus(bucketName); err != nil {
			http.Error(w, "Ошибка обновления статуса бакета", http.StatusInternalServerError)
			return
		}
	}

	// Возвращаем успешный ответ
	w.WriteHeader(http.StatusNoContent)
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
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	// Извлекаем имя бакета и объекта из пути
	pathSegments := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(pathSegments) < 2 {
		http.Error(w, "Неверный путь. Ожидалось /{bucketName}/{objectName}", http.StatusBadRequest)
		return
	}

	bucketName := pathSegments[0]
	objectName := strings.Join(pathSegments[1:], "/")

	// Проверяем существование бакета
	bucketDir := filepath.Join(BaseDir, bucketName)
	if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
		http.Error(w, "Бакет не найден", http.StatusNotFound)
		return
	}

	// Формируем путь к объекту
	objectPath := filepath.Join(bucketDir, objectName)

	// Проверяем существование объекта
	file, err := os.Open(objectPath)
	if os.IsNotExist(err) {
		http.Error(w, "Объект не найден", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Ошибка открытия объекта", http.StatusInternalServerError)
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
		http.Error(w, "Ошибка передачи объекта", http.StatusInternalServerError)
		return
	}
}
