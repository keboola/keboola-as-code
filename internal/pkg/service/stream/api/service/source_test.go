package service

import "testing"

func Test_formatAfterID(t *testing.T) {
	t.Parallel()

	type args struct {
		id string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{name: "valid_padded_id", args: args{id: "0000000001"}, want: "0000000001", wantErr: false},
		{name: "valid_unpadded_id", args: args{id: "12"}, want: "0000000012", wantErr: false},
		{name: "empty_string_id", args: args{id: ""}, want: "", wantErr: false},
		{name: "invalid_non_numeric_id", args: args{id: "dasd"}, want: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := formatAfterID(tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("formatAfterID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("formatAfterID() got = %v, want %v", got, tt.want)
			}
		})
	}
}
