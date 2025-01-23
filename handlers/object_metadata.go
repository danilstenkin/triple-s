package handlers

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var objectMetadataLock sync.Mutex

func AddObjectToMetadata(bucketName, objectName, contentType string, size int64, lastModified string) error {
	objectMetadataLock.Lock()
	defer objectMetadataLock.Unlock()

	metadataFilePath := filepath.Join(BaseDir, bucketName, "objects.csv")

	if _, err := os.Stat(metadataFilePath); os.IsNotExist(err) {
		file, err := os.Create(metadataFilePath)
		if err != nil {
			return fmt.Errorf("не удалось создать файл objects.csv: %v", err)
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		defer writer.Flush()

		if err := writer.Write([]string{"ObjectName", "Size", "ContentType", "LastModified"}); err != nil {
			return fmt.Errorf("не удалось записать заголовки в файл objects.csv: %v", err)
		}
	}

	file, err := os.OpenFile(metadataFilePath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("не удалось открыть файл objects.csv для записи: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	record := []string{objectName, fmt.Sprintf("%d", size), contentType, lastModified}
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("не удалось записать метаданные объекта: %v", err)
	}
	return nil
}

func isObjectInMetadata(bucketName, objectName string) (bool, error) {
	metadataFilePath := filepath.Join(BaseDir, bucketName, "objects.csv")

	file, err := os.Open(metadataFilePath)
	if err != nil {
		return false, fmt.Errorf("не удалось открыть файл метаданных объектов: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return false, fmt.Errorf("не удалось прочитать файл метаданных объектов: %v", err)
	}

	for _, record := range records {
		if len(record) > 0 && record[0] == objectName {
			return true, nil
		}
	}
	return false, nil
}

func UpdateObjectInMetadata(bucketName, objectName, contentType string, size int64, lastModified string) error {
	objectMetadataLock.Lock()
	defer objectMetadataLock.Unlock()

	metadataFilePath := filepath.Join(BaseDir, bucketName, "objects.csv")
	tempFilePath := metadataFilePath + ".tmp"

	file, err := os.Open(metadataFilePath)
	if err != nil {
		return fmt.Errorf("не удалось открыть файл objects.csv: %v", err)
	}
	defer file.Close()

	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("не удалось создать временный файл для objects.csv: %v", err)
	}
	defer tempFile.Close()

	reader := csv.NewReader(file)
	writer := csv.NewWriter(tempFile)
	defer writer.Flush()

	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("не удалось прочитать файл objects.csv: %v", err)
	}

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

	if !updated {
		newRecord := []string{objectName, fmt.Sprintf("%d", size), contentType, lastModified}
		if err := writer.Write(newRecord); err != nil {
			return fmt.Errorf("не удалось записать новую запись в временный файл: %v", err)
		}
	}

	if err := os.Rename(tempFilePath, metadataFilePath); err != nil {
		return fmt.Errorf("не удалось заменить файл objects.csv: %v", err)
	}
	return nil
}

func DeleteObjectFromMetadata(bucketName, objectName string) error {
	objectMetadataLock.Lock()
	defer objectMetadataLock.Unlock()

	metadataFilePath := filepath.Join(BaseDir, bucketName, "objects.csv")
	tempFilePath := metadataFilePath + ".tmp"

	file, err := os.Open(metadataFilePath)
	if err != nil {
		return fmt.Errorf("не удалось открыть файл objects.csv: %v", err)
	}
	defer file.Close()

	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("не удалось создать временный файл для objects.csv: %v", err)
	}
	defer tempFile.Close()

	reader := csv.NewReader(file)
	writer := csv.NewWriter(tempFile)
	defer writer.Flush()

	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("не удалось прочитать файл objects.csv: %v", err)
	}

	for _, record := range records {
		if len(record) > 0 && record[0] != objectName {
			if err := writer.Write(record); err != nil {
				return fmt.Errorf("не удалось записать запись в временный файл: %v", err)
			}
		}
	}

	if err := os.Rename(tempFilePath, metadataFilePath); err != nil {
		return fmt.Errorf("не удалось заменить файл objects.csv: %v", err)
	}
	return nil
}

func isBucketEmpty(bucketDir string) (bool, error) {
	entries, err := os.ReadDir(bucketDir)
	if err != nil {
		return false, err
	}

	for _, entry := range entries {
		if entry.Name() != "objects.csv" {
			return false, nil
		}
	}
	return true, nil
}
