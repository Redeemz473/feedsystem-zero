package logic

import (
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeBatchVideoEntityIDs(t *testing.T) {
	tests := []struct {
		name    string
		input   []uint64
		want    []uint64
		wantErr codes.Code
	}{
		{name: "empty", input: nil, want: []uint64{}, wantErr: codes.OK},
		{name: "filter zero and deduplicate", input: []uint64{9, 0, 3, 9, 5, 3}, want: []uint64{9, 3, 5}, wantErr: codes.OK},
		{name: "too many", input: make([]uint64, maxBatchVideoEntityIDs+1), wantErr: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeBatchVideoEntityIDs(tt.input)
			if status.Code(err) != tt.wantErr {
				t.Fatalf("unexpected error code: got %v want %v, err=%v", status.Code(err), tt.wantErr, err)
			}
			if tt.wantErr == codes.OK && !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("unexpected ids: got %v want %v", got, tt.want)
			}
		})
	}
}

func TestVideoEntityDBLoadKeyOrderIndependent(t *testing.T) {
	left := videoEntityDBLoadKey([]uint64{9, 2, 7})
	right := videoEntityDBLoadKey([]uint64{7, 9, 2})
	if left != right {
		t.Fatalf("same id set should share singleflight key: left=%q right=%q", left, right)
	}
}
