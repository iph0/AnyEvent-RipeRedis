use 5.008000;
use strict;
use warnings;

use Test::More;
use AnyEvent::RipeRedis qw( :err_codes );
require 't/test_helper.pl';

my $SERVER_INFO = run_redis_instance();
if ( !defined $SERVER_INFO ) {
  plan skip_all => 'redis-server is required for this test';
}
plan tests => 13;

my $R_CONSUM = AnyEvent::RipeRedis->new(
  host => $SERVER_INFO->{host},
  port => $SERVER_INFO->{port},
);
my $R_TRANSM = AnyEvent::RipeRedis->new(
  host => $SERVER_INFO->{host},
  port => $SERVER_INFO->{port},
);

t_subunsub( $R_CONSUM, $R_TRANSM );
t_psubunsub( $R_CONSUM, $R_TRANSM );

$R_CONSUM->disconnect;
$R_TRANSM->disconnect;

my $REDIS = AnyEvent::RipeRedis->new(
  host => $SERVER_INFO->{host},
  port => $SERVER_INFO->{port},
  on_error => sub {
    # do not print this errors
  },
);

t_sub_after_multi($REDIS);
t_sub_after_exec($REDIS);
t_sub_after_discard($REDIS);

$REDIS->disconnect;


sub t_subunsub {
  my $r_consum = shift;
  my $r_transm = shift;

  my $t_sub_reply;
  my @t_sub_msgs;

  ev_loop(
    sub {
      my $cv = shift;

      my $msg_cnt = 0;

      $r_consum->subscribe( qw( foo bar ),
        sub {
          my $msg     = shift;
          my $ch_name = shift;

          push( @t_sub_msgs,
            { message => $msg,
              ch_name => $ch_name,
            }
          );

          $msg_cnt++;
        }
      );

      $r_consum->subscribe( qw( events signals ),
        { on_reply => sub {
            $t_sub_reply = shift;
            my $err      = shift;

            if ( defined $err ) {
              diag( $err->message );
              return;
            }

            $r_transm->publish( 'foo',     'message_foo' );
            $r_transm->publish( 'bar',     'message_bar' );
            $r_transm->publish( 'events',  'message_events' );
            $r_transm->publish( 'signals', 'message_signals' );
          },

          on_message => sub {
            my $msg     = shift;
            my $ch_name = shift;

            push( @t_sub_msgs,
              { ch_name => $ch_name,
                message => $msg,
              }
            );

            if ( ++$msg_cnt == 4 ) {
              $cv->send;
            }
          },
        }
      );
    }
  );

  is( $t_sub_reply, 4, 'SUBSCRIBE' );

  is_deeply( \@t_sub_msgs,
    [ { message => 'message_foo',
        ch_name => 'foo',
      },
      { message => 'message_bar',
        ch_name => 'bar',
      },
      { message => 'message_events',
        ch_name => 'events',
      },

      { message => 'message_signals',
        ch_name => 'signals',
      },
    ],
    'SUBSCRIBE; publish message'
  );

  my $t_unsub_reply_1;
  my $t_unsub_reply_2;

  ev_loop(
    sub {
      my $cv = shift;

      $r_consum->unsubscribe( qw( foo bar ),
        sub {
          $t_unsub_reply_1 = shift;
          my $err          = shift;

          if ( defined $err ) {
            diag( $err->message );
            return;
          }
        }
      );

      $r_consum->unsubscribe(
        sub {
          $t_unsub_reply_2 = shift;
          my $err          = shift;

          if ( defined $err ) {
            diag( $err->message );
            return;
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_unsub_reply_1, 2, 'UNSUBSCRIBE; from specified channels' );
  is( $t_unsub_reply_2, 0, 'UNSUBSCRIBE; from all channels' );

  return;
}

sub t_psubunsub {
  my $r_consum = shift;
  my $r_transm = shift;

  my $t_psub_reply;
  my @t_sub_msgs;

  ev_loop(
    sub {
      my $cv = shift;

      my $msg_cnt = 0;

      $r_consum->psubscribe( qw( foo_* bar_* ),
        sub {
          my $msg        = shift;
          my $ch_pattern = shift;
          my $ch_name    = shift;

          push( @t_sub_msgs,
            { message    => $msg,
              ch_pattern => $ch_pattern,
              ch_name    => $ch_name,
            }
          );

          $msg_cnt++;
        }
      );

      $r_consum->psubscribe( qw( events_* signals_* ),
        { on_reply => sub {
            $t_psub_reply = shift;
            my $err       = shift;

            if ( defined $err ) {
              diag( $err->message );
              return;
            }

            $r_transm->publish( 'foo_test',     'message_foo_test' );
            $r_transm->publish( 'bar_test',     'message_bar_test' );
            $r_transm->publish( 'events_test',  'message_events_test' );
            $r_transm->publish( 'signals_test', 'message_signals_test' );
          },

          on_message => sub {
            my $msg        = shift;
            my $ch_pattern = shift;
            my $ch_name    = shift;

            push( @t_sub_msgs,
              { message    => $msg,
                ch_pattern => $ch_pattern,
                ch_name    => $ch_name,
              }
            );

            if ( ++$msg_cnt == 4 ) {
              $cv->send;
            }
          },
        }
      );
    }
  );

  is( $t_psub_reply, 4, 'PSUBSCRIBE' );

  is_deeply( \@t_sub_msgs,
    [ { message    => 'message_foo_test',
        ch_pattern => 'foo_*',
        ch_name    => 'foo_test',
      },
      { message    => 'message_bar_test',
        ch_pattern => 'bar_*',
        ch_name    => 'bar_test',
      },
      { message    => 'message_events_test',
        ch_pattern => 'events_*',
        ch_name    => 'events_test',
      },
      {
        message    => 'message_signals_test',
        ch_pattern => 'signals_*',
        ch_name    => 'signals_test',
      },
    ],
    'PSUBSCRIBE; publish message'
  );

  my $t_punsub_reply_1;
  my $t_punsub_reply_2;

  ev_loop(
    sub {
      my $cv = shift;

      $r_consum->punsubscribe( qw( foo_* bar_* ),
        sub {
          $t_punsub_reply_1 = shift;
          my $err           = shift;

          if ( defined $err ) {
            diag( $err->message );
            return;
          }
        }
      );

      $r_consum->punsubscribe(
        sub {
          $t_punsub_reply_2 = shift;
          my $err           = shift;

          if ( defined $err ) {
            diag( $err->message );
            return;
          }

          $cv->send;
        }
      );
    }
  );

  is( $t_punsub_reply_1, 2, 'PUNSUBSCRIBE; from specified patterns' );
  is( $t_punsub_reply_2, 0, 'PUNSUBSCRIBE; from all patterns' );

  return;
}

sub t_sub_after_multi {
  my $redis = shift;

  my $t_err;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->multi;
      $redis->subscribe( 'channel',
        { on_reply => sub {
            my $reply = shift;
            $t_err    = shift;

            $cv->send;
          },

          on_message => sub {},
        }
      );
    }
  );

  $redis->disconnect;

  my $t_pname = 'subscription after MULTI command';
  isa_ok( $t_err, 'AnyEvent::RipeRedis::Error' );
  is( $t_err->message, q{Command "subscribe" not allowed}
      . q{ after "multi" command. First, the transaction must be finalized.},
      "$t_pname; error message" );
  is( $t_err->code, E_OPRN_ERROR, "$t_pname; error code" );

  return;
}

sub t_sub_after_exec {
  my $redis = shift;

  my $t_reply;

  ev_loop(
    sub {
      my $cv = shift;

      $redis->set( 'foo', "some\r\nstring" );

      $redis->multi;
      $redis->get( 'foo' );
      $redis->incr( 'bar' );
      $redis->exec(
        sub {
          my $reply = shift;
          my $err   = shift;

          if ( defined $err ) {
            diag( $err->message );
          }
        }
      );

      $redis->subscribe( 'channel',
        { on_reply => sub {
            $t_reply = shift;
            my $err  = shift;

            if ( defined $err ) {
              diag( $err->message );
            }

            $redis->unsubscribe( 'channel',
              sub {
                my $reply = shift;
                my $err   = shift;

                if ( defined $err ) {
                  diag( $err->message );
                }

                $redis->del( qw( foo bar ),
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
          },

          on_message => sub {},
        }
      );
    }
  );

  is( $t_reply, 1, 'subscription after EXEC command' );

  return;
}

sub t_sub_after_discard {
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
          my $reply = shift;
          my $err   = shift;

          if ( defined $err ) {
            diag( $err->message );
          }
        }
      );

      $redis->subscribe( 'channel',
        { on_reply => sub {
            $t_reply = shift;
            my $err  = shift;

            if ( defined $err ) {
              diag( $err->message );
            }

            $redis->unsubscribe( 'channel',
              sub {
                my $reply = shift;
                my $err   = shift;

                if ( defined $err ) {
                  diag( $err->message );
                }

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
          },

          on_message => sub {},
        }
      );
    }
  );

  is( $t_reply, 1, 'subscription after DISCARD command' );

  return;
}
