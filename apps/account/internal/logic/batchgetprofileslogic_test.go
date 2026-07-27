package logic

import (
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeBatchProfileIDs(t *testing.T) {
	tests := []struct {
		name    string
		input   []uint64
		want    []uint64
		wantErr codes.Code
	}{
		{name: "empty", input: nil, want: []uint64{}, wantErr: codes.OK},
		{name: "filter zero and deduplicate", input: []uint64{3, 0, 2, 3, 1, 2}, want: []uint64{3, 2, 1}, wantErr: codes.OK},
		{name: "too many", input: make([]uint64, maxBatchProfileIDs+1), wantErr: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeBatchProfileIDs(tt.input)
			if status.Code(err) != tt.wantErr {
				t.Fatalf("unexpected error code: got %v want %v, err=%v", status.Code(err), tt.wantErr, err)
			}
			if tt.wantErr == codes.OK && !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("unexpected ids: got %v want %v", got, tt.want)
			}
		})
	}
}
