package server

import (
	"net/http"
	"testing"
)

func TestExtractAPIKey(t *testing.T) {
	cases := []struct {
		name   string
		header map[string]string
		want   string
	}{
		{"x-api-key", map[string]string{"X-API-Key": "secret"}, "secret"},
		{"bearer", map[string]string{"Authorization": "Bearer secret"}, "secret"},
		{"bearer lowercase scheme", map[string]string{"Authorization": "bearer secret"}, "secret"},
		{"bearer trims spaces", map[string]string{"Authorization": "Bearer   secret  "}, "secret"},
		{"x-api-key wins over bearer", map[string]string{"X-API-Key": "fromheader", "Authorization": "Bearer other"}, "fromheader"},
		{"non-bearer authorization ignored", map[string]string{"Authorization": "Basic abc"}, ""},
		{"empty bearer", map[string]string{"Authorization": "Bearer "}, ""},
		{"nothing", map[string]string{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, "/", nil)
			for k, v := range tc.header {
				req.Header.Set(k, v)
			}
			if got := extractAPIKey(req); got != tc.want {
				t.Errorf("extractAPIKey() = %q, want %q", got, tc.want)
			}
		})
	}
}
