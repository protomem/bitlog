package protocol_test

import (
	"errors"
	"reflect"
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

func TestParseOperation(t *testing.T) {
	type args struct {
		cmdRaw string
		args   []string
	}
	tests := []struct {
		name    string
		args    args
		want    protocol.Operation
		wantErr bool
		err     error
	}{
		{
			name: "Success: Ping",
			args: args{
				cmdRaw: protocol.PING.String(),
				args:   nil,
			},
			want: protocol.Operation{
				Cmd:  protocol.PING,
				Args: nil,
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "Success: Ping with args",
			args: args{
				cmdRaw: protocol.GET.String(),
				args:   []string{"foo"},
			},
			want: protocol.Operation{
				Cmd:  protocol.GET,
				Args: []string{"foo"},
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "Error: Ping with error: wrong number of args(2)",
			args: args{
				cmdRaw: protocol.PING.String(),
				args:   []string{"foo", "bar"},
			},
			want:    protocol.Operation{},
			wantErr: true,
			err:     protocol.ErrWrongNumberOfArguments,
		},
		{
			name: "Success: Get",
			args: args{
				cmdRaw: protocol.GET.String(),
				args:   []string{"foo"},
			},
			want: protocol.Operation{
				Cmd:  protocol.GET,
				Args: []string{"foo"},
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "Error: Get with error: wrong number of args(0)",
			args: args{
				cmdRaw: protocol.GET.String(),
				args:   nil,
			},
			want:    protocol.Operation{},
			wantErr: true,
			err:     protocol.ErrWrongNumberOfArguments,
		},
		{
			name: "Error: Get with error: wrong number of args(2)",
			args: args{
				cmdRaw: protocol.GET.String(),
				args:   []string{"foo", "bar"},
			},
			want:    protocol.Operation{},
			wantErr: true,
			err:     protocol.ErrWrongNumberOfArguments,
		},
		{
			name: "Success: Set",
			args: args{
				cmdRaw: protocol.SET.String(),
				args:   []string{"foo", "bar"},
			},
			want: protocol.Operation{
				Cmd:  protocol.SET,
				Args: []string{"foo", "bar"},
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "Error: Set with error: wrong number of args(1)",
			args: args{
				cmdRaw: protocol.SET.String(),
				args:   []string{"foo"},
			},
			want:    protocol.Operation{},
			wantErr: true,
			err:     protocol.ErrWrongNumberOfArguments,
		},
		{
			name: "Error: Set with error: wrong number of args(3)",
			args: args{
				cmdRaw: protocol.SET.String(),
				args:   []string{"foo", "bar", "baz"},
			},
			want:    protocol.Operation{},
			wantErr: true,
			err:     protocol.ErrWrongNumberOfArguments,
		},
		{
			name: "Success: Del",
			args: args{
				cmdRaw: protocol.DEL.String(),
				args:   []string{"foo"},
			},
			want: protocol.Operation{
				Cmd:  protocol.DEL,
				Args: []string{"foo"},
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "Success: Del with more args",
			args: args{
				cmdRaw: protocol.DEL.String(),
				args:   []string{"foo", "bar", "baz", "qux", "quux"},
			},
			want: protocol.Operation{
				Cmd:  protocol.DEL,
				Args: []string{"foo", "bar", "baz", "qux", "quux"},
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "Error: Del with error: wrong number of args(0)",
			args: args{
				cmdRaw: protocol.DEL.String(),
				args:   nil,
			},
			want:    protocol.Operation{},
			wantErr: true,
			err:     protocol.ErrWrongNumberOfArguments,
		},
		{
			name: "Success: Keys",
			args: args{
				cmdRaw: protocol.KEYS.String(),
				args:   []string{"foo"},
			},
			want: protocol.Operation{
				Cmd:  protocol.KEYS,
				Args: []string{"foo"},
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "Error: Keys with error: wrong number of args(0)",
			args: args{
				cmdRaw: protocol.KEYS.String(),
				args:   []string{"foo", "bar"},
			},
			want:    protocol.Operation{},
			wantErr: true,
			err:     protocol.ErrWrongNumberOfArguments,
		},
		{
			name: "Error: Unknown Command",
			args: args{
				cmdRaw: "unknown",
				args:   nil,
			},
			want:    protocol.Operation{},
			wantErr: true,
			err:     protocol.ErrUnknownCommand,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := protocol.ParseOperation(tt.args.cmdRaw, tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseOperation() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && !errors.Is(err, tt.err) {
				t.Errorf("ParseOperation() error = %v, expectedErr = %v", err, tt.err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseOperation() = %v, want %v", got, tt.want)
			}
		})
	}
}
