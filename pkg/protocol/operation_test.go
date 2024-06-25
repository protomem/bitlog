package protocol_test

import (
	"testing"

	"github.com/protomem/bitlog/pkg/protocol"
)

func TestParseCommand(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    protocol.Command
		wantErr bool
	}{
		{
			name: "Success: Command Ping",
			args: args{
				s: protocol.PING.String(),
			},
			want:    protocol.PING,
			wantErr: false,
		},
		{
			name: "Success: Command Get",
			args: args{
				s: protocol.GET.String(),
			},
			want:    protocol.GET,
			wantErr: false,
		},
		{
			name: "Success: Command Set",
			args: args{
				s: protocol.SET.String(),
			},
			want:    protocol.SET,
			wantErr: false,
		},
		{
			name: "Success: Command Del",
			args: args{
				s: protocol.DEL.String(),
			},
			want:    protocol.DEL,
			wantErr: false,
		},
		{
			name: "Success: Command Keys",
			args: args{
				s: protocol.KEYS.String(),
			},
			want:    protocol.KEYS,
			wantErr: false,
		},
		{
			name: "Error: Unknown Command",
			args: args{
				s: "unknown",
			},
			want:    protocol.UNKNOWN,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := protocol.ParseCommand(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCommand() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseCommand() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCommand_String(t *testing.T) {
	tests := []struct {
		name string
		cmd  protocol.Command
		want string
	}{
		{
			name: "Command Ping",
			cmd:  protocol.PING,
			want: "PING",
		},
		{
			name: "Command Get",
			cmd:  protocol.GET,
			want: "GET",
		},
		{
			name: "Command Set",
			cmd:  protocol.SET,
			want: "SET",
		},
		{
			name: "Command Del",
			cmd:  protocol.DEL,
			want: "DEL",
		},
		{
			name: "Command Keys",
			cmd:  protocol.KEYS,
			want: "KEYS",
		},
		{
			name: "Command Unknown",
			cmd:  protocol.UNKNOWN,
			want: "UNKNOWN",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cmd.String(); got != tt.want {
				t.Errorf("Command.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
