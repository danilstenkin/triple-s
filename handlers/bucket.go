package handlers

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Глобальная переменная для хранения пути к корневой директории
var BaseDir string

// Проверка валидности имени бакета
func isValidBucketName(bucketName string) bool {
	matched, _ := regexp.MatchString(`^[a-z0-9]([a-z0-9.-]{1,61}[a-z0-9])?$`, bucketName)
	return matched && !strings.Contains(bucketName, "..")
}

// Обработчик для создания бакета
func CreateBucketHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "PUT" {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	// Извлекаем имя бакета из пути
	bucketName := strings.TrimPrefix(r.URL.Path, "/buckets/")
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
		http.Error(w, "Бакет уже существует", http.StatusConflict)
		return
	}

	// Создаём директорию для бакета
	if err := os.MkdirAll(bucketDir, 0755); err != nil {
		http.Error(w, "Ошибка создания директории бакета", http.StatusInternalServerError)
		return
	}

	// Возвращаем успешный ответ
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Бакет '%s' успешно создан в '%s'", bucketName, bucketDir)
}
