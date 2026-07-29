package logic

import (
	"bytes"
	"testing"
)

func TestValidateUploadedFileSignature(t *testing.T) {
	tests := []struct {
		name    string
		ext     string
		content []byte
		wantErr bool
	}{
		{name: "jpeg", ext: ".jpg", content: []byte{0xff, 0xd8, 0xff, 0x00}},
		{name: "png", ext: ".png", content: []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}},
		{name: "webp", ext: ".webp", content: []byte("RIFF0000WEBP")},
		{name: "mp4", ext: ".mp4", content: []byte{0, 0, 0, 0, 'f', 't', 'y', 'p', 0, 0, 0, 0}},
		{name: "webm", ext: ".webm", content: []byte{0x1a, 0x45, 0xdf, 0xa3}},
		{name: "spoofed extension", ext: ".mp4", content: []byte("not a video"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUploadedFileSignature(bytes.NewReader(tt.content), tt.ext)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateUploadedFileSignature() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
