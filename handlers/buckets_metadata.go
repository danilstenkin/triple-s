package handlers

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var metadataLock sync.Mutex

var MetadataFilePath string

func InitializeMetadataFile(baseDir string) error {
	MetadataFilePath = filepath.Join(baseDir, "buckets.csv")

	if _, err := os.Stat(MetadataFilePath); os.IsNotExist(err) {
		file, err := os.Create(MetadataFilePath)
		if err != nil {
			return fmt.Errorf("не удалось создать файл метаданных: %v", err)
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		defer writer.Flush()
		err = writer.Write([]string{"Name", "CreationTime", "LastModified", "Status"})
		if err != nil {
			return fmt.Errorf("не удалось записать заголовки в файл метаданных: %v", err)
		}
	}
	return nil
}

func isBucketInMetadata(bucketName string) (bool, error) {
	file, err := os.Open(filepath.Join(BaseDir, "buckets.csv"))
	if err != nil {
		return false, fmt.Errorf("не удалось открыть файл метаданных: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return false, fmt.Errorf("не удалось прочитать файл метаданных: %v", err)
	}

	for _, record := range records {
		if len(record) > 0 && record[0] == bucketName {
			return true, nil
		}
	}
	return false, nil
}

func AddBucketToMetadata(bucketName, creationTime string) error {
	metadataLock.Lock()
	defer metadataLock.Unlock()

	file, err := os.OpenFile(MetadataFilePath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("не удалось открыть файл метаданных для записи: %v", err)
	}
	defer file.Close()

	status := "Inactive"

	writer := csv.NewWriter(file)
	defer writer.Flush()

	record := []string{bucketName, creationTime, creationTime, status}
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("не удалось записать метаданные бакета: %v", err)
	}
	return nil
}

func UpdateBucketStatus(bucketName string) error {
	metadataLock.Lock()
	defer metadataLock.Unlock()

	tempFilePath := MetadataFilePath + ".tmp"
	file, err := os.Open(MetadataFilePath)
	if err != nil {
		return fmt.Errorf("не удалось открыть файл метаданных: %v", err)
	}
	defer file.Close()

	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("не удалось создать временный файл для метаданных: %v", err)
	}
	defer tempFile.Close()

	reader := csv.NewReader(file)
	writer := csv.NewWriter(tempFile)
	defer writer.Flush()

	bucketDir := filepath.Join(BaseDir, bucketName)
	entries, err := os.ReadDir(bucketDir)
	if err != nil {
		return fmt.Errorf("не удалось прочитать содержимое бакета: %v", err)
	}

	status := "Inactive"
	if len(entries[1:]) > 0 {
		status = "Active"
	}

	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("не удалось прочитать файл метаданных: %v", err)
	}

	for _, record := range records {
		if len(record) > 0 && record[0] == bucketName {
			record[3] = status
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("не удалось записать запись в временный файл: %v", err)
		}
	}

	if err := os.Rename(tempFilePath, MetadataFilePath); err != nil {
		return fmt.Errorf("не удалось заменить файл метаданных: %v", err)
	}
	return nil
}

func RemoveBucketFromMetadata(bucketName string) error {
	metadataLock.Lock()
	defer metadataLock.Unlock()

	tempFilePath := MetadataFilePath + ".tmp"
	file, err := os.Open(MetadataFilePath)
	if err != nil {
		return fmt.Errorf("не удалось открыть файл метаданных: %v", err)
	}
	defer file.Close()

	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("не удалось создать временный файл для метаданных: %v", err)
	}
	defer tempFile.Close()

	reader := csv.NewReader(file)
	writer := csv.NewWriter(tempFile)
	defer writer.Flush()

	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("не удалось прочитать файл метаданных: %v", err)
	}

	for _, record := range records {
		if len(record) > 0 && record[0] != bucketName {
			if err := writer.Write(record); err != nil {
				return fmt.Errorf("не удалось записать запись в временный файл: %v", err)
			}
		}
	}

	if err := os.Rename(tempFilePath, MetadataFilePath); err != nil {
		return fmt.Errorf("не удалось заменить файл метаданных: %v", err)
	}
	return nil
}
