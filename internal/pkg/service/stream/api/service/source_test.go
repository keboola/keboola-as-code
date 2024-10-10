package service

import "testing"

func Test_formatAfterID(t *testing.T) {
	t.Parallel()

	type args struct {
		id string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "valid_padded_id", args: args{id: "0000000001"}, want: "0000000001"},
		{name: "valid_unpadded_id", args: args{id: "12"}, want: "0000000012"},
		{name: "empty_string_id", args: args{id: ""}, want: ""},
		{name: "invalid_non_numeric_id", args: args{id: "dasd"}, want: "000000dasd"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := formatAfterID(tt.args.id); got != tt.want {
				t.Errorf("formatAfterID() = %v, want %v", got, tt.want)
			}
		})
	}
}
