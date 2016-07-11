#!/usr/bin/perl

use strict;
use warnings;

use AnyEvent;
use AnyEvent::RipeRedis;

my $cv = AE::cv();

my $redis = AnyEvent::RipeRedis->new(
  host     => 'localhost',
  port     => 6379,
  password => 'redis_pass',

  on_connect => sub {
    print "Connected to Redis server\n";
  },

  on_disconnect => sub {
    print "Disconnected from Redis server\n";
  },
);

# Subscribe to channels by name
$redis->subscribe( qw( foo bar ),
  { on_reply => sub {
      my $reply = shift;
      my $err   = shift;

      if ( defined $err ) {
        warn $err->message . "\n";
        return;
      };

      print "Subscribed on: foo, bar\n";
    },

    on_message => sub {
      my $message = shift;
      my $channel = shift;

      print "$channel: $message\n";
    },
  }
);

# Subscribe to channels by pattern
$redis->psubscribe( qw( info_* err_* ),
  { on_reply => sub {
      my $reply = shift;
      my $err   = shift;

      if ( defined $err ) {
        warn $err->message . "\n";
        return;
      };

      print "Subscribed on: info_*, err_*\n";
    },

    on_message => sub {
      my $message = shift;
      my $pattern = shift;
      my $channel = shift;

      print "$pattern ($channel): $message\n";
    },
  }
);

# Unsubscribe
my $on_signal = sub {
  print "Stopped\n";

  $redis->unsubscribe( qw( foo bar ),
    sub {
      my $msg = shift;
      my $err = shift;

      if ( defined $err ) {
        warn $err->message . "\n";
        return;
      };

      print "Unsubscribed from: foo, bar\n";
    }
  );

  $redis->punsubscribe( qw( info_* err_* ),
    sub {
      my $msg = shift;
      my $err = shift;

      if ( defined $err ) {
        warn $err->message . "\n";
        return;
      };

      print "Unsubscribed from: info_*, err_*\n";

      $cv->send;
    }
  );

  my $timer;
  $timer = AE::timer( 5, 0,
    sub {
      undef( $timer );
      exit 0; # Emergency exit
    },
  );
};

my $int_w  = AE::signal( INT  => $on_signal );
my $term_w = AE::signal( TERM => $on_signal );

$cv->recv();

$redis->disconnect();
