package handlers

import (
	"encoding/xml"
	"net/http"
)

type XMLResponse struct {
	XMLName xml.Name `xml:"Response"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func WriteXMLResponse(w http.ResponseWriter, statusCode int, code string, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	errorResponse := XMLResponse{Code: code, Message: message}
	xml.NewEncoder(w).Encode(errorResponse)
}
