package handlers

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var objectMetadataLock sync.Mutex

// Добавление записи в object_metadata.csv
func AddObjectToMetadata(bucketName, objectName, contentType string, size int64, lastModified string) error {
	objectMetadataLock.Lock()
	defer objectMetadataLock.Unlock()

	// Путь к файлу object_metadata.csv
	metadataFilePath := filepath.Join(BaseDir, bucketName, "object_metadata.csv")

	// Если файл не существует, создаём его и добавляем заголовки
	if _, err := os.Stat(metadataFilePath); os.IsNotExist(err) {
		file, err := os.Create(metadataFilePath)
		if err != nil {
			return fmt.Errorf("не удалось создать файл object_metadata.csv: %v", err)
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		defer writer.Flush()

		if err := writer.Write([]string{"ObjectName", "Size", "ContentType", "LastModified"}); err != nil {
			return fmt.Errorf("не удалось записать заголовки в файл object_metadata.csv: %v", err)
		}
	}

	// Открываем файл для добавления записи
	file, err := os.OpenFile(metadataFilePath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("не удалось открыть файл object_metadata.csv для записи: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Записываем данные объекта
	record := []string{objectName, fmt.Sprintf("%d", size), contentType, lastModified}
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("не удалось записать метаданные объекта: %v", err)
	}
	return nil
}

// Обновление записи в object_metadata.csv
func UpdateObjectInMetadata(bucketName, objectName, contentType string, size int64, lastModified string) error {
	objectMetadataLock.Lock()
	defer objectMetadataLock.Unlock()

	metadataFilePath := filepath.Join(BaseDir, bucketName, "object_metadata.csv")
	tempFilePath := metadataFilePath + ".tmp"

	// Открываем оригинальный файл и временный файл
	file, err := os.Open(metadataFilePath)
	if err != nil {
		return fmt.Errorf("не удалось открыть файл object_metadata.csv: %v", err)
	}
	defer file.Close()

	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("не удалось создать временный файл для object_metadata.csv: %v", err)
	}
	defer tempFile.Close()

	reader := csv.NewReader(file)
	writer := csv.NewWriter(tempFile)
	defer writer.Flush()

	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("не удалось прочитать файл object_metadata.csv: %v", err)
	}

	// Обновляем или добавляем запись
	updated := false
	for _, record := range records {
		if len(record) > 0 && record[0] == objectName {
			record[1] = fmt.Sprintf("%d", size)
			record[2] = contentType
			record[3] = lastModified
			updated = true
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("не удалось записать запись в временный файл: %v", err)
		}
	}

	// Если запись не была найдена, добавляем её
	if !updated {
		newRecord := []string{objectName, fmt.Sprintf("%d", size), contentType, lastModified}
		if err := writer.Write(newRecord); err != nil {
			return fmt.Errorf("не удалось записать новую запись в временный файл: %v", err)
		}
	}

	// Заменяем оригинальный файл временным
	if err := os.Rename(tempFilePath, metadataFilePath); err != nil {
		return fmt.Errorf("не удалось заменить файл object_metadata.csv: %v", err)
	}
	return nil
}

// Удаление записи из object_metadata.csv
func DeleteObjectFromMetadata(bucketName, objectName string) error {
	objectMetadataLock.Lock()
	defer objectMetadataLock.Unlock()

	metadataFilePath := filepath.Join(BaseDir, bucketName, "object_metadata.csv")
	tempFilePath := metadataFilePath + ".tmp"

	file, err := os.Open(metadataFilePath)
	if err != nil {
		return fmt.Errorf("не удалось открыть файл object_metadata.csv: %v", err)
	}
	defer file.Close()

	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("не удалось создать временный файл для object_metadata.csv: %v", err)
	}
	defer tempFile.Close()

	reader := csv.NewReader(file)
	writer := csv.NewWriter(tempFile)
	defer writer.Flush()

	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("не удалось прочитать файл object_metadata.csv: %v", err)
	}

	// Копируем все записи, кроме удаляемой
	for _, record := range records {
		if len(record) > 0 && record[0] != objectName {
			if err := writer.Write(record); err != nil {
				return fmt.Errorf("не удалось записать запись в временный файл: %v", err)
			}
		}
	}

	// Заменяем оригинальный файл временным
	if err := os.Rename(tempFilePath, metadataFilePath); err != nil {
		return fmt.Errorf("не удалось заменить файл object_metadata.csv: %v", err)
	}
	return nil
}

func isBucketEmpty(bucketDir string) (bool, error) {
	entries, err := os.ReadDir(bucketDir)
	if err != nil {
		return false, err
	}

	// Проверяем, содержит ли бакет только object_metadata.csv
	for _, entry := range entries {
		if entry.Name() != "object_metadata.csv" {
			return false, nil
		}
	}
	return true, nil
}
