package AnyEvent::RipeRedis;

use 5.008000;
use strict;
use warnings;
use base qw( Exporter );

our $VERSION = '0.04';

use AnyEvent::RipeRedis::Error;

use AnyEvent;
use AnyEvent::Handle;
use Scalar::Util qw( looks_like_number weaken );
use Digest::SHA qw( sha1_hex );
use Carp qw( croak );

our %ERROR_CODES;

BEGIN {
  %ERROR_CODES = (
    E_CANT_CONN                  => 1,
    E_LOADING_DATASET            => 2,
    E_IO                         => 3,
    E_CONN_CLOSED_BY_REMOTE_HOST => 4,
    E_CONN_CLOSED_BY_CLIENT      => 5,
    E_NO_CONN                    => 6,
    E_OPRN_ERROR                 => 9,
    E_UNEXPECTED_DATA            => 10,
    E_NO_SCRIPT                  => 11,
    E_READ_TIMEDOUT              => 12,
    E_BUSY                       => 13,
    E_MASTER_DOWN                => 14,
    E_MISCONF                    => 15,
    E_READONLY                   => 16,
    E_OOM                        => 17,
    E_EXEC_ABORT                 => 18,
    E_NO_AUTH                    => 19,
    E_WRONG_TYPE                 => 20,
    E_NO_REPLICAS                => 21,
    E_BUSY_KEY                   => 22,
    E_CROSS_SLOT                 => 23,
    E_TRY_AGAIN                  => 24,
    E_ASK                        => 25,
    E_MOVED                      => 26,
    E_CLUSTER_DOWN               => 27,
  );
}

BEGIN {
  our @EXPORT_OK   = keys %ERROR_CODES;
  our %EXPORT_TAGS = ( err_codes => \@EXPORT_OK, );
}

use constant {
  # Default values
  D_HOST     => 'localhost',
  D_PORT     => 6379,
  D_DB_INDEX => 0,

  %ERROR_CODES,

  # Operation status
  S_NEED_PERFORM => 1,
  S_IN_PROGRESS  => 2,
  S_DONE         => 3,

  # String terminator
  EOL        => "\r\n",
  EOL_LENGTH => 2,
};

my %SUB_COMMANDS = (
  subscribe  => 1,
  psubscribe => 1,
);

my %SUBUNSUB_COMMANDS = (
  %SUB_COMMANDS,
  unsubscribe  => 1,
  punsubscribe => 1,
);

my %MESSAGE_TYPES = (
  message  => 1,
  pmessage => 1,
);

my %ERR_PREFS_MAP = (
  LOADING     => E_LOADING_DATASET,
  NOSCRIPT    => E_NO_SCRIPT,
  BUSY        => E_BUSY,
  MASTERDOWN  => E_MASTER_DOWN,
  MISCONF     => E_MISCONF,
  READONLY    => E_READONLY,
  OOM         => E_OOM,
  EXECABORT   => E_EXEC_ABORT,
  NOAUTH      => E_NO_AUTH,
  WRONGTYPE   => E_WRONG_TYPE,
  NOREPLICAS  => E_NO_REPLICAS,
  BUSYKEY     => E_BUSY_KEY,
  CROSSSLOT   => E_CROSS_SLOT,
  TRYAGAIN    => E_TRY_AGAIN,
  ASK         => E_ASK,
  MOVED       => E_MOVED,
  CLUSTERDOWN => E_CLUSTER_DOWN,
);

my %EVAL_CACHE;


sub new {
  my $class  = shift;
  my %params = @_;

  my $self = bless {}, $class;

  $self->{host} = $params{host} || D_HOST;
  $self->{port} = $params{port} || D_PORT;
  $self->{password} = $params{password};
  $self->{database}
      = defined $params{database} ? $params{database} : D_DB_INDEX;
  $self->{utf8}      = exists $params{utf8}      ? $params{utf8}      : 1;
  $self->{reconnect} = exists $params{reconnect} ? $params{reconnect} : 1;
  $self->{handle_params}    = $params{handle_params} || {};
  $self->{on_connect}       = $params{on_connect};
  $self->{on_disconnect}    = $params{on_disconnect};
  $self->{on_connect_error} = $params{on_connect_error};

  $self->connection_timeout( $params{connection_timeout} );
  $self->read_timeout( $params{read_timeout} );
  $self->min_reconnect_interval( $params{min_reconnect_interval} );
  $self->on_error( $params{on_error} );

  $self->{_handle}           = undef;
  $self->{_connected}        = 0;
  $self->{_lazy_conn_state}  = $params{lazy};
  $self->{_auth_state}       = S_NEED_PERFORM;
  $self->{_select_db_state}  = S_NEED_PERFORM;
  $self->{_ready}            = 0;
  $self->{_input_queue}      = [];
  $self->{_temp_queue}       = [];
  $self->{_processing_queue} = [];
  $self->{_txn_lock}         = 0;
  $self->{_channels}         = {};
  $self->{_channel_cnt}      = 0;
  $self->{_pchannel_cnt}     = 0;
  $self->{_reconnect_timer}  = undef;

  unless ( $self->{_lazy_conn_state} ) {
    $self->_connect;
  }

  return $self;
}

sub info {
  my $self = shift;
  my $cmd  = $self->_prepare_command( 'info', [@_] );

  weaken($self);
  my $on_reply = $cmd->{on_reply};

  $cmd->{on_reply} = sub {
    my $reply = shift;
    my $err   = shift;

    if ( defined $err ) {
      $on_reply->( undef, $err );
      return;
    }

    $reply = { map { split( m/:/, $_, 2 ) }
        grep { m/^[^#]/ } split( EOL, $reply ) };

    $on_reply->($reply);
  };

  $self->_execute_command($cmd);

  return;
}

sub select {
  my $self = shift;
  my $cmd  = $self->_prepare_command( 'select', [@_] );

  weaken($self);

  my $on_reply = $cmd->{on_reply};
  my $database = $cmd->{args}[0];

  $cmd->{on_reply} = sub {
    my $reply = shift;
    my $err   = shift;

    if ( defined $err ) {
      $on_reply->( undef, $err );
      return;
    }

    $self->{database} = $database;

    $on_reply->($reply);
  };

  $self->_execute_command($cmd);

  return;
}

sub multi {
  my $self = shift;
  my $cmd  = $self->_prepare_command( 'multi', [@_] );

  $self->{_txn_lock} = 1;
  $self->_execute_command($cmd);

  return;
}

sub exec {
  my $self = shift;
  my $cmd  = $self->_prepare_command( 'exec', [@_] );

  $self->{_txn_lock} = 0;
  $self->_execute_command($cmd);

  return;
}

sub eval_cached {
  my $self = shift;
  my $cmd  = $self->_prepare_command( 'evalsha', [@_] );

  my $script = $cmd->{args}[0];
  unless ( exists $EVAL_CACHE{$script} ) {
    $EVAL_CACHE{$script} = sha1_hex($script);
  }
  $cmd->{args}[0] = $EVAL_CACHE{$script};

  {
    weaken($self);
    weaken( my $cmd = $cmd );

    my $on_reply = $cmd->{on_reply};

    $cmd->{on_reply} = sub {
      my $reply = shift;
      my $err   = shift;

      if ( defined $err ) {
        if ( $err->code == E_NO_SCRIPT ) {
          $cmd->{kwd}     = 'eval';
          $cmd->{args}[0] = $script;

          $self->_push_write($cmd);

          return;
        }

        $on_reply->( $reply, $err );

        return;
      }

      $on_reply->($reply);
    };
  }

  $self->_execute_command($cmd);

  return;
}

sub quit {
  my $self = shift;
  my $cmd  = $self->_prepare_command( 'quit', [@_] );

  weaken($self);
  my $on_reply = $cmd->{on_reply};

  $cmd->{on_reply} = sub {
    my $reply = shift;
    my $err   = shift;

    if ( defined $err ) {
      $on_reply->( undef, $err );
      return;
    }

    $self->_disconnect;

    $on_reply->($reply);
  };

  $self->_execute_command($cmd);

  return;
}

sub disconnect {
  my $self = shift;

  $self->_disconnect;

  return;
}

# Generate sub/unsub methods
{
  no strict 'refs';

  foreach my $kwd ( keys %SUBUNSUB_COMMANDS ) {
    *{$kwd} = sub {
      my $self = shift;
      my $cmd = $self->_prepare_command( $kwd, [@_] );

      if ( exists $SUB_COMMANDS{ $cmd->{kwd} }
        && !defined $cmd->{on_message} )
      {
        croak q{"on_message" callback must be specified};
      }

      if ( $self->{_txn_lock} ) {
        AE::postpone(
          sub {
            my $err = _new_error(
              qq{Command "$cmd->{kwd}" not allowed after "multi" command.}
                  . ' First, the transaction must be finalized.',
              E_OPRN_ERROR
            );

            $cmd->{on_reply}->( undef, $err );
          }
        );

        return;
      }

      if ( @{ $cmd->{args} } ) {
        $cmd->{reply_cnt} = scalar @{ $cmd->{args} };
      }

      $self->_execute_command($cmd);

      return;
    };
  }
}

sub host {
  my $self = shift;
  return $self->{host};
}

sub port {
  my $self = shift;
  return $self->{port};
}

sub selected_database {
  my $self = shift;
  return $self->{database};
}

sub on_error {
  my $self = shift;

  if (@_) {
    my $on_error = shift;

    if ( defined $on_error ) {
      $self->{on_error} = $on_error;
    }
    else {
      $self->{on_error} = sub {
        my $err = shift;
        warn $err->message . "\n";
      };
    }
  }

  return $self->{on_error};
}

# Generate accessors
{
  no strict 'refs';

  foreach my $name ( qw( connection_timeout read_timeout
      min_reconnect_interval ) )
  {
    *{$name} = sub {
      my $self = shift;

      if (@_) {
        my $seconds = shift;

        if ( defined $seconds
          && ( !looks_like_number($seconds) || $seconds < 0 ) )
        {
          croak qq{"$name" must be a positive number};
        }
        $self->{$name} = $seconds;
      }

      return $self->{$name};
    };
  }

  foreach my $name ( qw( utf8 reconnect on_connect on_disconnect
      on_connect_error ) )
  {
    *{$name} = sub {
      my $self = shift;

      if (@_) {
        $self->{$name} = shift;
      }

      return $self->{$name};
    };
  }
}

sub _connect {
  my $self = shift;

  $self->{_handle} = AnyEvent::Handle->new(
    %{ $self->{handle_params} },
    connect          => [ $self->{host}, $self->{port} ],
    on_prepare       => $self->_get_on_prepare,
    on_connect       => $self->_get_on_connect,
    on_connect_error => $self->_get_on_connect_error,
    on_rtimeout      => $self->_get_on_rtimeout,
    on_eof           => $self->_get_on_eof,
    on_error         => $self->_get_handle_on_error,
    on_read          => $self->_get_on_read,
  );

  return;
}

sub _get_on_prepare {
  my $self = shift;

  weaken($self);

  return sub {
    if ( defined $self->{connection_timeout} ) {
      return $self->{connection_timeout};
    }

    return;
  };
}

sub _get_on_connect {
  my $self = shift;

  weaken($self);

  return sub {
    $self->{_connected} = 1;

    unless ( defined $self->{password} ) {
      $self->{_auth_state} = S_DONE;
    }
    if ( $self->{database} == 0 ) {
      $self->{_select_db_state} = S_DONE;
    }

    if ( $self->{_auth_state} == S_NEED_PERFORM ) {
      $self->_auth;
    }
    elsif ( $self->{_select_db_state} == S_NEED_PERFORM ) {
      $self->_select_database;
    }
    else {
      $self->{_ready} = 1;
      $self->_flush_input_queue;
    }

    if ( defined $self->{on_connect} ) {
      $self->{on_connect}->();
    }
  };
}

sub _get_on_connect_error {
  my $self = shift;

  weaken($self);

  return sub {
    my $err_msg = pop;

    my $err = _new_error(
      "Can't connect to $self->{host}:$self->{port}: $err_msg",
      E_CANT_CONN
    );

    $self->_disconnect($err);
  };
}

sub _get_on_rtimeout {
  my $self = shift;

  weaken($self);

  return sub {
    if ( @{ $self->{_processing_queue} } ) {
      my $err = _new_error( 'Read timed out.', E_READ_TIMEDOUT );

      $self->_disconnect($err);
    }
    else {
      $self->{_handle}->rtimeout(undef);
    }
  };
}

sub _get_on_eof {
  my $self = shift;

  weaken($self);

  return sub {
    my $err = _new_error( 'Connection closed by remote host.',
        E_CONN_CLOSED_BY_REMOTE_HOST );

    $self->_disconnect($err);
  };
}

sub _get_handle_on_error {
  my $self = shift;

  weaken($self);

  return sub {
    my $err_msg = pop;

    my $err = _new_error( $err_msg, E_IO );
    $self->_disconnect($err);
  };
}

sub _get_on_read {
  my $self = shift;

  weaken($self);

  my $str_len;
  my @bufs;
  my $bufs_num = 0;

  return sub {
    my $handle = shift;

    MAIN: while (1) {
      if ( $handle->destroyed ) {
        return;
      }

      my $reply;
      my $err_code;

      if ( defined $str_len ) {
        if ( length( $handle->{rbuf} ) < $str_len + EOL_LENGTH ) {
          return;
        }

        $reply = substr( $handle->{rbuf}, 0, $str_len, '' );
        substr( $handle->{rbuf}, 0, EOL_LENGTH, '' );
        if ( $self->{utf8} ) {
          utf8::decode($reply);
        }

        undef $str_len;
      }
      else {
        my $eol_pos = index( $handle->{rbuf}, EOL );

        if ( $eol_pos < 0 ) {
          return;
        }

        $reply = substr( $handle->{rbuf}, 0, $eol_pos, '' );
        my $type = substr( $reply, 0, 1, '' );
        substr( $handle->{rbuf}, 0, EOL_LENGTH, '' );

        if ( $type ne '+' && $type ne ':' ) {
          if ( $type eq '$' ) {
            if ( $reply >= 0 ) {
              $str_len = $reply;
              next;
            }

            undef $reply;
          }
          elsif ( $type eq '*' ) {
            if ( $reply > 0 ) {
              push( @bufs,
                { reply      => [],
                  err_code   => undef,
                  chunks_cnt => $reply,
                }
              );
              $bufs_num++;

              next;
            }
            elsif ( $reply == 0 ) {
              $reply = [];
            }
            else {
              undef $reply;
            }
          }
          elsif ( $type eq '-' ) {
            $err_code = E_OPRN_ERROR;
            if ( $reply =~ m/^([A-Z]{3,}) / ) {
              if ( exists $ERR_PREFS_MAP{$1} ) {
                $err_code = $ERR_PREFS_MAP{$1};
              }
            }
          }
          else {
            my $err = _new_error( 'Unexpected reply received.',
                E_UNEXPECTED_DATA );

            $self->_disconnect($err);

            return;
          }
        }
      }

      while ( $bufs_num > 0 ) {
        my $curr_buf = $bufs[-1];
        if ( defined $err_code ) {
          unless ( ref($reply) ) {
            $reply = _new_error( $reply, $err_code );
          }
          $curr_buf->{err_code} = E_OPRN_ERROR;
        }
        push( @{ $curr_buf->{reply} }, $reply );
        if ( --$curr_buf->{chunks_cnt} > 0 ) {
          next MAIN;
        }

        $reply    = $curr_buf->{reply};
        $err_code = $curr_buf->{err_code};
        pop @bufs;
        $bufs_num--;
      }

      $self->_process_reply( $reply, $err_code );
    }

    return;
  };
}

sub _prepare_command {
  my $self = shift;
  my $kwd  = shift;
  my $args = shift;

  my $cmd;
  if ( ref( $args->[-1] ) eq 'HASH' ) {
    $cmd = pop @{$args};
  }
  else {
    $cmd = {};
    if ( ref( $args->[-1] ) eq 'CODE' ) {
      if ( exists $SUB_COMMANDS{$kwd} ) {
        $cmd->{on_message} = pop @{$args};
      }
      else {
        $cmd->{on_reply} = pop @{$args};
      }
    }
  }
  $cmd->{kwd}  = $kwd;
  $cmd->{args} = $args;

  unless ( defined $cmd->{on_reply} ) {
    weaken($self);

    $cmd->{on_reply} = sub {
      my $reply = shift;
      my $err   = shift;

      if ( defined $err ) {
        $self->{on_error}->( $err, $reply );
        return;
      }
    }
  }

  return $cmd;
}

sub _execute_command {
  my $self = shift;
  my $cmd  = shift;

  unless ( $self->{_ready} ) {
    if ( defined $self->{_handle} ) {
      if ( $self->{_connected} ) {
        if ( $self->{_auth_state} == S_DONE ) {
          if ( $self->{_select_db_state} == S_NEED_PERFORM ) {
            $self->_select_database;
          }
        }
        elsif ( $self->{_auth_state} == S_NEED_PERFORM ) {
          $self->_auth;
        }
      }
    }
    elsif ( $self->{_lazy_conn_state} ) {
      $self->{_lazy_conn_state} = 0;
      $self->_connect;
    }
    elsif ( $self->{reconnect} ) {
      if ( defined $self->{min_reconnect_interval}
        && $self->{min_reconnect_interval} > 0 )
      {
        unless ( defined $self->{_reconnect_timer} ) {
          $self->{_reconnect_timer} = AE::timer(
            $self->{min_reconnect_interval}, 0,
            sub {
              undef $self->{_reconnect_timer};
              $self->_connect;
            }
          );
        }
      }
      else {
        $self->_connect;
      }
    }
    else {
      AE::postpone(
        sub {
          my $err = _new_error(
            qq{Operation "$cmd->{kwd}" aborted: No connection to the server.},
            E_NO_CONN
          );

          $cmd->{on_reply}->( undef, $err );
        }
      );

      return;
    }

    push( @{ $self->{_input_queue} }, $cmd );

    return;
  }

  $self->_push_write($cmd);

  return;
}

sub _push_write {
  my $self = shift;
  my $cmd  = shift;

  my $cmd_str = '';
  foreach my $token ( $cmd->{kwd}, @{ $cmd->{args} } ) {
    unless ( defined $token ) {
      $token = '';
    }
    elsif ( $self->{utf8} ) {
      utf8::encode($token);
    }
    $cmd_str .= '$' . length($token) . EOL . $token . EOL;
  }
  $cmd_str = '*' . ( scalar( @{ $cmd->{args} } ) + 1 ) . EOL . $cmd_str;

  my $handle = $self->{_handle};

  if ( defined $self->{read_timeout} && !@{ $self->{_processing_queue} } ) {
    $handle->rtimeout_reset;
    $handle->rtimeout( $self->{read_timeout} );
  }

  push( @{ $self->{_processing_queue} }, $cmd );
  $handle->push_write($cmd_str);

  return;
}

sub _auth {
  my $self = shift;

  weaken($self);
  $self->{_auth_state} = S_IN_PROGRESS;

  $self->_push_write(
    { kwd  => 'auth',
      args => [ $self->{password} ],

      on_reply => sub {
        my $err = $_[1];

        if ( defined $err ) {
          $self->{_auth_state} = S_NEED_PERFORM;
          $self->_abort($err);

          return;
        }

        $self->{_auth_state} = S_DONE;

        if ( $self->{_select_db_state} == S_NEED_PERFORM ) {
          $self->_select_database;
        }
        else {
          $self->{_ready} = 1;
          $self->_flush_input_queue;
        }
      },
    }
  );

  return;
}

sub _select_database {
  my $self = shift;

  weaken($self);
  $self->{_select_db_state} = S_IN_PROGRESS;

  $self->_push_write(
    { kwd  => 'select',
      args => [ $self->{database} ],

      on_reply => sub {
        my $err = $_[1];

        if ( defined $err ) {
          $self->{_select_db_state} = S_NEED_PERFORM;
          $self->_abort($err);

          return;
        }

        $self->{_select_db_state} = S_DONE;
        $self->{_ready}           = 1;

        $self->_flush_input_queue;
      },
    }
  );

  return;
}

sub _flush_input_queue {
  my $self = shift;

  $self->{_temp_queue}  = $self->{_input_queue};
  $self->{_input_queue} = [];

  while ( my $cmd = shift @{ $self->{_temp_queue} } ) {
    $self->_push_write($cmd);
  }

  return;
}

sub _process_reply {
  my $self     = shift;
  my $reply    = shift;
  my $err_code = shift;

  if ( defined $err_code ) {
    $self->_process_error( $reply, $err_code );
  }
  elsif ( $self->{_channel_cnt} + $self->{_pchannel_cnt} > 0
    && ref($reply) && exists $MESSAGE_TYPES{ $reply->[0] } )
  {
    $self->_process_message($reply);
  }
  else {
    $self->_process_success($reply);
  }

  return;
}

sub _process_error {
  my $self     = shift;
  my $reply    = shift;
  my $err_code = shift;

  my $cmd = shift @{ $self->{_processing_queue} };

  unless ( defined $cmd ) {
    my $err = _new_error(
      q{Don't know how process error message. Processing queue is empty.},
      E_UNEXPECTED_DATA
    );

    $self->_disconnect($err);

    return;
  }

  if ( ref($reply) ) {
    my $err = _new_error(
        qq{Operation "$cmd->{kwd}" completed with errors.}, $err_code );

    $cmd->{on_reply}->( $reply, $err );
  }
  else {
    my $err = _new_error( $reply, $err_code );
    $cmd->{on_reply}->( undef, $err );
  }

  return;
}

sub _process_message {
  my $self = shift;
  my $msg  = shift;

  my $cmd = $self->{_channels}{ $msg->[1] };

  unless ( defined $cmd ) {
    my $err = _new_error(
      q{Don't know how process published message.}
          . qq{ Unknown channel or pattern "$msg->[1]".},
      E_UNEXPECTED_DATA
    );

    $self->_disconnect($err);

    return;
  }

  $cmd->{on_message}->( $msg->[0] eq 'pmessage' ? @{$msg}[ 3, 1, 2 ]
      : @{$msg}[ 2, 1 ] );

  return;
}

sub _process_success {
  my $self  = shift;
  my $reply = shift;

  my $cmd = $self->{_processing_queue}[0];

  unless ( defined $cmd ) {
    my $err = _new_error(
      q{Don't know how process reply. Processing queue is empty.},
      E_UNEXPECTED_DATA
    );

    $self->_disconnect($err);

    return;
  }

  if ( exists $SUBUNSUB_COMMANDS{ $cmd->{kwd} } ) {
    if ( $cmd->{kwd} eq 'subscribe' ) {
      $self->{_channels}{ $reply->[1] } = $cmd;
      $self->{_channel_cnt}++;
    }
    elsif ( $cmd->{kwd} eq 'psubscribe' ) {
      $self->{_channels}{ $reply->[1] } = $cmd;
      $self->{_pchannel_cnt}++;
    }
    elsif ( $cmd->{kwd} eq 'unsubscribe' ) {
      unless ( defined $cmd->{reply_cnt} ) {
        $cmd->{reply_cnt} = $self->{_channel_cnt};
      }

      delete $self->{_channels}{ $reply->[1] };
      $self->{_channel_cnt}--;
    }
    else {    # punsubscribe
      unless ( defined $cmd->{reply_cnt} ) {
        $cmd->{reply_cnt} = $self->{_pchannel_cnt};
      }

      delete $self->{_channels}{ $reply->[1] };
      $self->{_pchannel_cnt}--;
    }

    $reply = $reply->[2];
  }

  if ( !defined $cmd->{reply_cnt} || --$cmd->{reply_cnt} == 0 ) {
    shift @{ $self->{_processing_queue} };
    $cmd->{on_reply}->($reply);
  }

  return;
}

sub _disconnect {
  my $self = shift;
  my $err  = shift;

  my $was_connected = $self->{_connected};

  if ( defined $self->{_handle} ) {
    $self->{_handle}->destroy;
    undef $self->{_handle};
  }
  $self->{_connected}       = 0;
  $self->{_auth_state}      = S_NEED_PERFORM;
  $self->{_select_db_state} = S_NEED_PERFORM;
  $self->{_ready}           = 0;
  $self->{_txn_lock}        = 0;

  $self->_abort($err);

  if ( $was_connected && defined $self->{on_disconnect} ) {
    $self->{on_disconnect}->($self);
  }

  return;
}

sub _abort {
  my $self = shift;
  my $err  = shift;

  my @unfin_cmds = (
    @{ $self->{_processing_queue} },
    @{ $self->{_temp_queue} },
    @{ $self->{_input_queue} },
  );
  my %channels = %{ $self->{_channels} };

  $self->{_input_queue}      = [];
  $self->{_temp_queue}       = [];
  $self->{_processing_queue} = [];
  $self->{_channels}         = {};
  $self->{_channel_cnt}      = 0;
  $self->{_pchannel_cnt}     = 0;

  if ( !defined $err && @unfin_cmds ) {
    $err = _new_error( 'Connection closed by client prematurely.',
        E_CONN_CLOSED_BY_CLIENT );
  }

  if ( defined $err ) {
    my $err_msg  = $err->message;
    my $err_code = $err->code;

    if ( defined $self->{on_connect_error} && $err_code == E_CANT_CONN ) {
      $self->{on_connect_error}->($err);
    }
    else {
      $self->{on_error}->($err);
    }

    if ( %channels && $err_code != E_CONN_CLOSED_BY_CLIENT ) {
      foreach my $name ( keys %channels ) {
        my $err = _new_error(
          qq{Subscription to channel "$name" lost: $err_msg},
          $err_code
        );

        my $cmd = $channels{$name};
        $cmd->{on_reply}->( undef, $err );
      }
    }

    foreach my $cmd (@unfin_cmds) {
      my $err = _new_error( qq{Operation "$cmd->{kwd}" aborted: $err_msg},
          $err_code );

      $cmd->{on_reply}->( undef, $err );
    }
  }

  return;
}

sub _new_error {
  return AnyEvent::RipeRedis::Error->new(@_);
}

sub AUTOLOAD {
  our $AUTOLOAD;
  my $method = $AUTOLOAD;
  $method =~ s/^.+:://;
  my ( $kwd, @extra_args ) = split( m/_/, lc($method) );

  my $sub = sub {
    my $self = shift;
    my $cmd  = $self->_prepare_command( $kwd, [ @extra_args, @_ ] );

    $self->_execute_command($cmd);

    return;
  };

  do {
    no strict 'refs';
    *{$method} = $sub;
  };

  goto &{$sub};
}

sub DESTROY {
  my $self = shift;

  if ( defined $self->{_processing_queue} ) {
    my @unfin_cmds = (
      @{ $self->{_processing_queue} },
      @{ $self->{_temp_queue} },
      @{ $self->{_input_queue} },
    );

    foreach my $cmd ( @unfin_cmds ) {
      warn qq{Operation "$cmd->{kwd}" aborted:}
          . " Client object destroyed prematurely.\n";
    }
  }

  return;
}

1;
__END__

=head1 NAME

AnyEvent::RipeRedis - Flexible non-blocking Redis client with reconnect
feature

=head1 SYNOPSIS

  use AnyEvent;
  use AnyEvent::RipeRedis;

  my $redis = AnyEvent::RipeRedis->new(
    host     => 'localhost',
    port     => 6379,
    password => 'yourpass',
  );

  my $cv = AE::cv;

  $redis->get( 'foo',
    sub {
      my $reply = shift;
      my $err   = shift;

      if ( defined $err ) {
        my $err_msg  = $err->message;
        my $err_code = $err->code;

        warn "[$err_code] $err_msg\n";
        $cv->send;

        return;
      }

      print "$reply\n";
      $cv->send;
    }
  );

  $cv->recv;

=head1 DESCRIPTION

AnyEvent::RipeRedis is flexible non-blocking Redis client with reconnect
feature. The client supports subscriptions, transactions and connection via
UNIX-socket.

Requires Redis 1.2 or higher, and any supported event loop.

=head1 CONSTRUCTOR

=head2 new( %params )

  my $redis = AnyEvent::RipeRedis->new(
    host                   => 'localhost',
    port                   => 6379,
    password               => 'yourpass',
    database               => 7,
    lazy                   => 1,
    connection_timeout     => 5,
    read_timeout           => 5,
    min_reconnect_interval => 5,

    on_connect => sub {
      # handling...
    },

    on_disconnect => sub {
      # handling...
    },

    on_connect_error => sub {
      my $err = shift;

      # error handling...
    },

    on_error => sub {
      my $err = shift;

      # error handling...
    },
  );

=over

=item host => $host

Server hostname (default: 127.0.0.1)

=item port => $port

Server port (default: 6379)

=item password => $password

If the password is specified, the C<AUTH> command is sent to the server
after connection.

=item database => $index

Database index. If the index is specified, the client is switched to
the specified database after connection. You can also switch to another database
after connection by using C<SELECT> command. The client remembers last selected
database after reconnection.

The default database index is C<0>.

=item utf8 => $boolean

If enabled, all strings will be converted to UTF-8 before sending to server,
and all results will be decoded from UTF-8.

Enabled by default.

=item connection_timeout => $fractional_seconds

Specifies connection timeout. If the client could not connect to the server
after specified timeout, the C<on_connect_error> callback is called with the
C<E_CANT_CONN> error, or if it not specified, the C<on_error> callback is
called. The timeout specifies in seconds and can contain a fractional part.

  connection_timeout => 10.5,

By default the client use kernel's connection timeout.

=item read_timeout => $fractional_seconds

Specifies read timeout. If the client could not receive a reply from the server
after specified timeout, the client close connection and call the C<on_error>
callback with the C<E_READ_TIMEDOUT> error. The timeout is specifies in seconds
and can contain a fractional part.

  read_timeout => 3.5,

Not set by default.

=item lazy => $boolean

If enabled, the connection establishes at time when you will send the first
command to the server. By default the connection establishes after calling of
the C<new> method.

Disabled by default.

=item reconnect => $boolean

If the connection to the Redis server was lost and the parameter C<reconnect> is
TRUE, the client try to restore the connection on a next command execution
unless C<min_reconnect_interval> is specified. The client try to reconnect only
once and if it fails, is called the C<on_error> callback. If you need several
attempts of the reconnection, just retry a command from the C<on_error>
callback as many times, as you need. This feature made the client more responsive.

Enabled by default.

=item min_reconnect_interval => $fractional_seconds

If the parameter is specified, the client will try to reconnect not often, than
after this interval.

  min_reconnect_interval => 5,

Not set by default.

=item handle_params => \%params

Specifies L<AnyEvent::Handle> parameters. Enabling of the C<autocork> parameter
can improve perfomance.

  handle_params => {
    autocork => 1,
    linger   => 60,
  }

=item on_connect => $cb->()

The C<on_connect> callback is called when the connection is successfully
established.

Not set by default.

=item on_disconnect => $cb->()

The C<on_disconnect> callback is called when the connection is closed by any
reason.

Not set by default.

=item on_connect_error => $cb->( $err )

The C<on_connect_error> callback is called, when the connection could not be
established. If this callback isn't specified, the C<on_error> callback is
called.

Not set by default.

=item on_error => $cb->( $err )

The C<on_error> callback is called when occurred an error, which was affected
on whole client (e. g. connection error or authentication error). Also the
C<on_error> callback is called on command errors if the command callback is not
specified. If the C<on_error> callback is not specified, the client just print
an error messages to C<STDERR>.

=back

=head1 COMMAND EXECUTION

=head2 <command>( [ @args ] [, $cb->( $reply, $err ) ] )

The full list of the Redis commands can be found here: L<http://redis.io/commands>.

The reply to the command is passed to the callback in first argument. If any
error occurred during command execution, the error object is passed to the
callback in second argument. Error object is the instance of the class
L<AnyEvent::RipeRedis::Error>.

The command callback is optional. If it is not specified and any error
occurred, the C<on_error> callback of the client is called.

  $redis->set( 'foo', 'string' );

  $redis->get( 'foo',
    sub {
      my $reply = shift;
      my $err   = shift;

      if ( defined $err ) {
        # error handling...

        return;
      }

      print "$reply\n";
    }
  );

  $redis->lrange( 'list', 0, -1,
    sub {
      my $reply = shift;
      my $err   = shift;

      if ( defined $err ) {
        # error handling...

        return;
      }

      foreach my $value ( @{$reply}  ) {
        print "$value\n";
      }
    }
  );

You can execute multi-word commands like this:

  $redis->client_getname(
    sub {
      my $reply = shift;
      my $err   = shift;

      if ( defined $err ) {
        # error handling...

        return;
      }

      print "$reply\n";
    }
  );

=head1 TRANSACTIONS

The detailed information about the Redis transactions can be found here:
L<http://redis.io/topics/transactions>.

=head2 multi( [ $cb->( $reply, $err ) ] )

Marks the start of a transaction block. Subsequent commands will be queued for
atomic execution using C<EXEC>.

=head2 exec( [ $cb->( $reply, $err ) ] )

Executes all previously queued commands in a transaction and restores the
connection state to normal. When using C<WATCH>, C<EXEC> will execute commands
only if the watched keys were not modified.

If during a transaction at least one command fails, to the callback of C<exec()>
method will be passed error object and the reply will be contain nested error
objects for every failed command.

  $redis->multi();
  $redis->set( 'foo', 'string' );
  $redis->incr('foo');    # causes an error
  $redis->exec(
    sub {
      my $reply = shift;
      my $err   = shift;

      if ( defined $err ) {
        my $err_msg  = $err->message();
        my $err_code = $err->code();

        if ( defined $reply ) {
          foreach my $nested_reply ( @{$reply} ) {
            if ( ref($nested_reply) eq 'AnyEvent::RipeRedis::Error' ) {
              my $nested_err_msg  = $nested_reply->message();
              my $nested_err_code = $nested_reply->code();

              # error handling...
            }
          }

          return;
        }

        # error handling...

        return;
      }

      # reply handling...
    },
  );

=head2 discard( [ $cb->( $reply, $err ) ] )

Flushes all previously queued commands in a transaction and restores the
connection state to normal.

If C<WATCH> was used, C<DISCARD> unwatches all keys.

=head2 watch( @keys [, $cb->( $reply, $err ) ] )

Marks the given keys to be watched for conditional execution of a transaction.

=head2 unwatch( [ $cb->( $reply, $err ) ] )

Forget about all watched keys.

=head1 SUBSCRIPTIONS

Once the client enters the subscribed state it is not supposed to issue any
other commands, except for additional C<SUBSCRIBE>, C<PSUBSCRIBE>,
C<UNSUBSCRIBE>, C<PUNSUBSCRIBE> and C<QUIT> commands.

The detailed information about Redis Pub/Sub can be found here:
L<http://redis.io/topics/pubsub>

=head2 subscribe( @channels, ( $cb | \%cbs ) )

Subscribes the client to the specified channels.

Method can accept two callbacks: C<on_reply> and C<on_message>. The C<on_reply>
callback is called when subscription to all specified channels will be
activated. In first argument to the callback is passed the number of channels
we are currently subscribed. If subscription to specified channels was lost,
the C<on_reply> callback is called with the error object in the second argument.

The C<on_message> callback is called on every published message. If the
C<subscribe> method is called with one callback, this callback will be act as
C<on_message> callback.

  $redis->subscribe( qw( foo bar ),
    { on_reply => sub {
        my $channels_num = shift;
        my $err          = shift;

        if ( defined $err ) {
          # error handling...

          return;
        }

        # reply handling...
      },

      on_message => sub {
        my $msg     = shift;
        my $channel = shift;

        # message handling...
      },
    }
  );

  $redis->subscribe( qw( foo bar ),
    sub {
      my $msg     = shift;
      my $channel = shift;

      # message handling...
    }
  );

=head2 psubscribe( @patterns, ( $cb | \%cbs ) )

Subscribes the client to the given patterns. See C<subscribe()> method for
details.

  $redis->psubscribe( qw( foo_* bar_* ),
    { on_reply => sub {
        my $channels_num = shift;
        my $err          = shift;

        if ( defined $err ) {
          # error handling...

          return;
        }

        # reply handling...
      },

      on_message => sub {
        my $msg     = shift;
        my $pattern = shift;
        my $channel = shift;

        # message handling...
      },
    }
  );

  $redis->psubscribe( qw( foo_* bar_* ),
    sub {
      my $msg     = shift;
      my $pattern = shift;
      my $channel = shift;

      # message handling...
    }
  );

=head2 publish( $channel, $message [, $cb->( $reply, $err ) ] )

Posts a message to the given channel.

=head2 unsubscribe( [ @channels ] [, $cb->( $reply, $err ) ] )

Unsubscribes the client from the given channels, or from all of them if none
is given. In first argument to the callback is passed the number of channels we
are currently subscribed or zero if we were unsubscribed from all channels.

  $redis->unsubscribe( qw( foo bar ),
    sub {
      my $channels_num = shift;
      my $err          = shift;

      if ( defined $err ) {
        # error handling...

        return;
      }

      # reply handling...
    }
  );

=head2 punsubscribe( [ @patterns ] [, $cb->( $reply, $err ) ] )

Unsubscribes the client from the given patterns, or from all of them if none
is given. See C<unsubscribe()> method for details.


  $redis->punsubscribe( qw( foo_* bar_* ),
    sub {
      my $channels_num = shift;
      my $err          = shift;

      if ( defined $err ) {
        # error handling...

        return;
      }

      # reply handling...
    }
  );

=head1 CONNECTION VIA UNIX-SOCKET

Redis 2.2 and higher support connection via UNIX domain socket. To connect via
a UNIX-socket in the parameter C<host> you have to specify C<unix/>, and in
the parameter C<port> you have to specify the path to the socket.

  my $redis = AnyEvent::RipeRedis->new(
    host => 'unix/',
    port => '/tmp/redis.sock',
  );

=head1 LUA SCRIPTS EXECUTION

Redis 2.6 and higher support execution of Lua scripts on the server side.
To execute a Lua script you can send one of the commands C<EVAL> or C<EVALSHA>,
or use the special method C<eval_cached()>.

=head2 eval_cached( $script, $numkeys [, @keys ] [, @args ] [, $cb->( $reply, $err ) ] ] );

When you call the C<eval_cached()> method, the client first generate a SHA1
hash for a Lua script and cache it in memory. Then the client optimistically
send the C<EVALSHA> command under the hood. If the C<E_NO_SCRIPT> error will be
returned, the client send the C<EVAL> command.

If you call the C<eval_cached()> method with the same Lua script, client don not
generate a SHA1 hash for this script repeatedly, it gets a hash from the cache
instead.

  $redis->eval_cached( 'return { KEYS[1], KEYS[2], ARGV[1], ARGV[2] }',
      2, 'key1', 'key2', 'first', 'second',
    sub {
      my $reply = shift;
      my $err   = shift;

      if ( defined $err ) {
        # error handling...

        return;
      }

      foreach my $value ( @{$reply}  ) {
        print "$value\n";
      }
    }
  );

Be care, passing a different Lua scripts to C<eval_cached()> method every time
cause memory leaks.

If Lua script returns multi-bulk reply with at least one error reply, to the
callback of C<eval_cached> method will be passed error object and the reply
will be contain nested error objects.

  $redis->eval_cached( "return { 'foo', redis.error_reply( 'Error.' ) }", 0,
    sub {
      my $reply = shift;
      my $err   = shift;

      if ( defined $err ) {
        my $err_msg  = $err->message;
        my $err_code = $err->code;

        if ( defined $reply ) {
          foreach my $nested_reply ( @{$reply} ) {
            if ( ref($nested_reply) eq 'AnyEvent::RipeRedis::Error' ) {
              my $nested_err_msg  = $nested_reply->message();
              my $nested_err_code = $nested_reply->code();

              # error handling...
            }
          }
        }

        # error handling...

        return;
      }

      # reply handling...
    }
  );

=head1 ERROR CODES

Error codes can be used for programmatic handling of errors.
AnyEvent::RipeRedis provides constants of error codes, which can be imported
and used in expressions.

  use AnyEvent::RipeRedis qw( :err_codes );

=over

=item E_CANT_CONN

Can't connect to the server. All operations were aborted.

=item E_LOADING_DATASET

Redis is loading the dataset in memory.

=item E_IO

Input/Output operation error. The connection to the Redis server was closed and
all operations were aborted.

=item E_CONN_CLOSED_BY_REMOTE_HOST

The connection closed by remote host. All operations were aborted.

=item E_CONN_CLOSED_BY_CLIENT

Connection closed by client prematurely. Uncompleted operations were aborted.

=item E_NO_CONN

No connection to the Redis server. Connection was lost by any reason on previous
operation.

=item E_OPRN_ERROR

Operation error. For example, wrong number of arguments for a command.

=item E_UNEXPECTED_DATA

The client received unexpected reply from the server. The connection to the Redis
server was closed and all operations were aborted.

=item E_READ_TIMEDOUT

Read timed out. The connection to the Redis server was closed and all operations
were aborted.

=back

Error codes available since Redis 2.6.

=over

=item E_NO_SCRIPT

No matching script. Use the C<EVAL> command.

=item E_BUSY

Redis is busy running a script. You can only call C<SCRIPT KILL>
or C<SHUTDOWN NOSAVE>.

=item E_MASTER_DOWN

Link with MASTER is down and slave-serve-stale-data is set to 'no'.

=item E_MISCONF

Redis is configured to save RDB snapshots, but is currently not able to persist
on disk. Commands that may modify the data set are disabled. Please check Redis
logs for details about the error.

=item E_READONLY

You can't write against a read only slave.

=item E_OOM

Command not allowed when used memory > 'maxmemory'.

=item E_EXEC_ABORT

Transaction discarded because of previous errors.

=back

Error codes available since Redis 2.8.

=over

=item E_NO_AUTH

Authentication required.

=item E_WRONG_TYPE

Operation against a key holding the wrong kind of value.

=item E_NO_REPLICAS

Not enough good slaves to write.

=item E_BUSY_KEY

Target key name already exists.

=back

Error codes available since Redis 3.0.

=over

=item E_CROSS_SLOT

Keys in request don't hash to the same slot.

=item E_TRY_AGAIN

Multiple keys request during rehashing of slot.

=item E_ASK

Redirection required. For more information see:
L<http://redis.io/topics/cluster-spec>

=item E_MOVED

Redirection required. For more information see:
L<http://redis.io/topics/cluster-spec>

=item E_CLUSTER_DOWN

The cluster is down or hash slot not served.

=back

=head1 DISCONNECTION

When the connection to the server is no longer needed you can close it in three
ways: call the method C<disconnect()>, send the C<QUIT> command or you can just
"forget" any references to an AnyEvent::RipeRedis object, but in this
case a client object is destroyed without calling any callbacks, including
the C<on_disconnect> callback, to avoid an unexpected behavior.

=head2 disconnect()

The method for synchronous disconnection. All uncompleted operations will be
aborted.

  $redis->disconnect();

=head2 quit( [ $cb->( $reply, $err ) ] )

The method for asynchronous disconnection.

=head1 OTHER METHODS

=head2 info( [ $section ] [, $cb->( $reply, $err ) ] )

Gets and parses information and statistics about the server. The result
is passed to callback as a hash reference.

More information abount C<INFO> command can be found here:
L<http://redis.io/commands/info>

=head2 host()

Get current host of the client.

=head2 port()

Get current port of the client.

=head2 select( $index, [, $cb->( $reply, $err ) ] )

Selects the database by numeric index.

=head2 selected_database()

Get selected database index.

=head2 utf8( [ $boolean ] )

Enables or disables UTF-8 mode.

=head2 connection_timeout( [ $fractional_seconds ] )

Get or set the C<connection_timeout> of the client. The C<undef> value resets
the C<connection_timeout> to default value.

=head2 read_timeout( [ $fractional_seconds ] )

Get or set the C<read_timeout> of the client.

=head2 reconnect( [ $boolean ] )

Enables or disables reconnection mode of the client.

=head2 min_reconnect_interval( [ $fractional_seconds ] )

Get or set C<min_reconnect_interval> of the client.

=head2 on_connect( [ $callback ] )

Get or set the C<on_connect> callback.

=head2 on_disconnect( [ $callback ] )

Get or set the C<on_disconnect> callback.

=head2 on_connect_error( [ $callback ] )

Get or set the C<on_connect_error> callback.

=head2 on_error( [ $callback ] )

Get or set the C<on_error> callback.

=head1 SEE ALSO

L<AnyEvent>, L<Redis::hiredis>, L<Redis>, L<RedisDB>

=head1 AUTHOR

Eugene Ponizovsky, E<lt>ponizovsky@gmail.comE<gt>

=head2 Special thanks

=over

=item *

Alexey Shrub

=item *

Vadim Vlasov

=item *

Konstantin Uvarin

=item *

Ivan Kruglov

=back

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2012-2016, Eugene Ponizovsky, E<lt>ponizovsky@gmail.comE<gt>.
All rights reserved.

This module is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut
