use 5.008000;
use strict;
use warnings;
use utf8;

use Test::More;
use AnyEvent::RipeRedis qw( :err_codes );
require 't/test_helper.pl';

my $SERVER_INFO = run_redis_instance();
if ( !defined $SERVER_INFO ) {
  plan skip_all => 'redis-server is required for this test';
}
plan tests => 33;

my $REDIS;
my $T_IS_CONN = 0;
my $T_IS_DISCONN = 0;

ev_loop(
  sub {
    my $cv = shift;

    $REDIS = AnyEvent::RipeRedis->new(
      host               => $SERVER_INFO->{host},
      port               => $SERVER_INFO->{port},
      connection_timeout => 5,
      read_timeout       => 5,
      encoding           => 'utf8',
      handle_params      => {
        autocork => 1,
      },

      on_connect => sub {
        $T_IS_CONN = 1;
        $cv->send;
      },
      on_disconnect => sub {
        $T_IS_DISCONN = 1;
      },
    );
  },
);

ok( $T_IS_CONN, 'on_connect' );

t_status_reply($REDIS);
t_numeric_reply($REDIS);
t_bulk_reply($REDIS);
t_set_undef($REDIS);
t_get_undef($REDIS);
t_set_utf8_string($REDIS);
t_get_utf8_string($REDIS);
t_get_non_existent($REDIS);
t_mbulk_reply($REDIS);
t_mbulk_reply_empty_list($REDIS);
t_mbulk_reply_undef($REDIS);
t_nested_mbulk_reply($REDIS);
t_multi_word_command($REDIS);
t_oprn_error($REDIS);
t_default_on_error($REDIS);
t_error_after_exec($REDIS);
t_discard($REDIS);
t_execute($REDIS);
t_quit($REDIS);


sub t_status_reply {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'foo', "some\r\nstring",
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }
        }
      );

      $redis->del( 'foo',
        sub {
          my $reply = shift;
          my $err   = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_reply, 'OK', 'SET; status reply' );

  return;
}

sub t_numeric_reply {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->incr( 'bar',
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }
        }
      );

      $redis->del( 'bar',
        sub {
          my $reply = shift;
          my $err   = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_reply, 1, 'INCR; numeric reply' );

  return;
}

sub t_bulk_reply {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'foo', "some\r\nstring" );

      $redis->get( 'foo',
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }
        }
      );

      $redis->del( 'foo',
        sub {
          my $reply = shift;
          my $err   = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_reply, "some\r\nstring", 'GET; bulk reply' );

  return;
}

sub t_set_undef {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'empty', undef,
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_reply, 'OK', 'SET; undef' );

  return;
}

sub t_get_undef {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'empty',
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_reply, '', 'GET; undef' );

  return;
}

sub t_set_utf8_string {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'ключ', 'Значение',
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }
        }
      );

      $redis->del( 'ключ',
        sub {
          my $reply = shift;
          my $err   = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_reply, 'OK', 'SET; UTF-8 string' );

  return;
}

sub t_get_utf8_string {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'ключ', 'Значение' );

      $redis->get( 'ключ',
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }
        }
      );

      $redis->del( 'ключ',
        sub {
          my $reply = shift;
          my $err   = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_reply, 'Значение', 'GET; UTF-8 string' );

  return;
}

sub t_get_non_existent {
  my $redis = shift;

  my $t_reply = 'not_undef';
  my $t_err;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->get( 'non_existent',
        sub {
          $t_reply = shift;
          $t_err   = shift;

          if ( defined $t_err ) {
            diag( $t_err->message );
          }

          $cv->send;
        }
      );
    }
  );

  ok( !defined $t_reply && !defined $t_err, 'GET; non existent key' );

  return;
}

sub t_mbulk_reply {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      for ( my $i = 1; $i <= 3; $i++ ) {
        $redis->rpush( 'list', "element_$i" );
      }

      $redis->lrange( 'list', 0, -1,
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( $err ) {
            diag( $err->message );
          }
        }
      );

      $redis->del( 'list',
        sub {
          my $reply = shift;
          my $err   = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is_deeply( $t_reply,
    [ qw(
        element_1
        element_2
        element_3
      )
    ],
    'LRANGE; multi-bulk reply'
  );

  return;
}

sub t_mbulk_reply_empty_list {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->lrange( 'non_existent', 0, -1,
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is_deeply( $t_reply, [], 'LRANGE; empty list' );

  return;
}

sub t_mbulk_reply_undef {
  my $redis = shift;

  my $t_reply = 'not_undef';
  my $t_err;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->brpop( 'non_existent', '1',
        sub {
          $t_reply = shift;
          $t_err   = shift;

          if ( defined $t_err ) {
            diag( $t_err->message );
          }

          $cv->send;
        }
      );
    }
  );

  ok( !defined $t_reply && !defined $t_err, 'BRPOP; multi-bulk undef' );

  return;
}

sub t_nested_mbulk_reply {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      for ( my $i = 1; $i <= 3; $i++ ) {
        $redis->rpush( 'list', "element_$i" );
      }

      $redis->set( 'bar', "some\r\nstring" );

      $redis->multi;
      $redis->incr( 'foo' );
      $redis->lrange( 'list', 0, -1 );
      $redis->lrange( 'non_existent', 0, -1 );
      $redis->get( 'bar' );
      $redis->lrange( 'list', 0, -1 );
      $redis->exec(
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }
        }
      );

      $redis->del( qw( foo bar list ),
        sub {
          my $reply = shift;
          my $err   = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is_deeply( $t_reply,
    [ 1,
      [ qw(
          element_1
          element_2
          element_3
        )
      ],
      [],
      "some\r\nstring",
      [ qw(
          element_1
          element_2
          element_3
        )
      ],
    ],
    'EXEC; nested multi-bulk reply'
  );

  return;
}

sub t_multi_word_command {
  my $redis = shift;

  my $ver = get_redis_version($REDIS);

  SKIP: {
    if ( $ver < version->parse( 'v2.6.9' ) ) {
      skip 'redis-server 2.6.9 or higher is required for this test', 2;
    }

    my $t_reply;

    ev_loop(
      sub {
        my $cv = shift;

        $redis->client_setname( 'test',
          sub {
            $t_reply = shift;
            my $err  = shift;

            if ( defined $err ) {
              diag( $err->message );
            }

            $cv->send;
          }
        );
      }
    );

    is_deeply( $t_reply, 'OK', 'CLIENT SETNAME; multiple word command' );

    ev_loop(
      sub {
        my $cv = shift;

        $redis->client_getname(
          sub {
            $t_reply = shift;
            my $err  = shift;

            if ( defined $err ) {
              diag( $err->message );
            }

            $cv->send;
          }
        );
      }
    );

    is_deeply( $t_reply, 'test', 'CLIENT GETNAME; multiple word command' );
  }

  return;
}

sub t_oprn_error {
  my $redis = shift;

  my $t_err;

  ev_loop(
    sub {
      my $cv = shift;

      # missing argument
      $redis->set( 'foo',
        sub {
          my $reply = shift;
          $t_err    = shift;

          $cv->send;
        }
      );
    }
  );

  my $t_npref = 'operation error;';
  isa_ok( $t_err, 'AnyEvent::RipeRedis::Error' );
  ok( defined $t_err->message, "$t_npref; error message" );
  is( $t_err->code, E_OPRN_ERROR, "$t_npref; error code" );

  return;
}

sub t_default_on_error {
  my $redis = shift;

  my $cv;
  my $t_err_msg;

  local $SIG{__WARN__} = sub {
    $t_err_msg = shift;

    chomp( $t_err_msg );

    $cv->send;
  };

  ev_loop(
    sub {
      $cv = shift;

      $redis->set; # missing argument
    }
  );

  ok( defined $t_err_msg, q{Default 'on_error' callback} );

  return;
}

sub t_error_after_exec {
  my $redis = shift;

  my $t_reply;
  my $t_err;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->multi;
      $redis->set( 'foo', 'string' );
      $redis->incr( 'foo' );
      $redis->exec(
        sub {
          $t_reply = shift;
          $t_err   = shift;

          $cv->send;
        }
      );
    }
  );

  my $t_npref = 'error after EXEC;';
  isa_ok( $t_err, 'AnyEvent::RipeRedis::Error' );
  is( $t_err->message, q{Operation "exec" completed with errors.},
      "$t_npref; error message" );
  is( $t_err->code, E_OPRN_ERROR, "$t_npref; error code" );
  is( $t_reply->[0], 'OK', "$t_npref; status reply" );

  isa_ok( $t_reply->[1], 'AnyEvent::RipeRedis::Error' );
  can_ok( $t_reply->[1], 'code' );
  can_ok( $t_reply->[1], 'message' );
  ok( defined $t_reply->[1]->message, "$t_npref; nested error message" );
  is( $t_reply->[1]->code, E_OPRN_ERROR, "$t_npref; nested error message" );

  return;
}

sub t_discard {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'foo', "some\r\nstring" );

      $redis->multi;
      $redis->get( 'foo' );
      $redis->incr( 'bar' );
      $redis->discard(
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }
        }
      );

      $redis->del( 'foo',
        sub {
          my $reply = shift;
          my $err   = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_reply, 'OK', 'DISCARD; status reply' );

  return;
}

sub t_execute {
  my $redis = shift;

  can_ok( $redis, 'execute' );

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->execute( 'set', 'foo', "some\r\nstring",
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }
        }
      );

      $redis->execute( 'del', 'foo',
        sub {
          my $reply = shift;
          my $err   = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_reply, 'OK', 'execute; status reply' );

  return;
}

sub t_quit {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->quit(
        sub {
          $t_reply = shift;
          my $err  = shift;

          if ( defined $err ) {
            diag( $err->message );
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_reply, 'OK', 'QUIT; status reply; disconnect' );
  ok( $T_IS_DISCONN, 'on_disconnect' );

  return;
}
