package handlers

import (
	"encoding/xml"
	"net/http"
)

type ErrorResponse struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func WriteXMLError(w http.ResponseWriter, statusCode int, code string, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	errorResponse := ErrorResponse{Code: code, Message: message}
	xml.NewEncoder(w).Encode(errorResponse)
}
