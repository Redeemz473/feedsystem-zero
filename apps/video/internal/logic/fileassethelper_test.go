package logic

import (
	"reflect"
	"testing"
)

func TestOrderedFileAssetURLs(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "sorts lock order",
			in:   []string{"/uploads/video_b.mp4", "/uploads/video_a.mp4"},
			want: []string{"/uploads/video_a.mp4", "/uploads/video_b.mp4"},
		},
		{
			name: "keeps duplicate logical references",
			in:   []string{"/uploads/shared.mp4", "/uploads/shared.mp4"},
			want: []string{"/uploads/shared.mp4", "/uploads/shared.mp4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := orderedFileAssetURLs(tt.in...)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("orderedFileAssetURLs() = %v, want %v", got, tt.want)
			}
		})
	}
}
