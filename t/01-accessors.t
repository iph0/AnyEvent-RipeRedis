use 5.008000;
use strict;
use warnings;

use lib 't/tlib';
use Test::More tests => 41;
use AnyEvent::RipeRedis qw( :err_codes );

my $REDIS = AnyEvent::RipeRedis->new(
  password               => 'test',
  connection_timeout     => 10,
  read_timeout           => 5,
  reconnect              => 1,
  min_reconnect_interval => 5,
  encoding               => 'utf8',

  on_connect => sub {
    return 1;
  },

  on_disconnect => sub {
    return 2;
  },

  on_connect_error => sub {
    return 3;
  },

  on_error => sub {
    return 4;
  },
);

can_ok( $REDIS, 'host' );
can_ok( $REDIS, 'port' );
can_ok( $REDIS, 'connection_timeout' );
can_ok( $REDIS, 'read_timeout' );
can_ok( $REDIS, 'selected_database' );
can_ok( $REDIS, 'utf8' );
can_ok( $REDIS, 'reconnect' );
can_ok( $REDIS, 'on_connect' );
can_ok( $REDIS, 'on_disconnect' );
can_ok( $REDIS, 'on_connect_error' );
can_ok( $REDIS, 'on_error' );

t_host($REDIS);
t_port($REDIS);
t_conn_timeout($REDIS);
t_read_timeout($REDIS);
t_reconnect($REDIS);
t_min_reconnect_interval($REDIS);
t_utf8($REDIS);
t_on_connect($REDIS);
t_on_disconnect($REDIS);
t_on_connect_error($REDIS);
t_on_error($REDIS);
t_database($REDIS);


sub t_host {
  my $redis = shift;

  my $t_host = $redis->host;

  is( $t_host, 'localhost', 'get host' );

  return;
}

sub t_port {
  my $redis = shift;

  my $t_port = $redis->port;

  is( $t_port, 6379, 'get port' );

  return;
}

sub t_conn_timeout {
  my $redis = shift;

  my $t_conn_timeout = $redis->connection_timeout;
  is( $t_conn_timeout, 10, q{get "connection_timeout"} );

  $redis->connection_timeout(undef);
  is( $redis->connection_timeout, undef,
    q{reset to default "connection_timeout"} );

  $redis->connection_timeout(15);
  is( $redis->connection_timeout, 15, q{set "connection_timeout"} );

  return;
}

sub t_read_timeout {
  my $redis = shift;

  my $t_read_timeout = $redis->read_timeout;
  is( $t_read_timeout, 5, q{get "read_timeout"} );

  $redis->read_timeout(undef);
  is( $redis->read_timeout, undef, q{disable "read_timeout"} );

  $redis->read_timeout(10);
  is( $redis->read_timeout, 10, q{set "read_timeout"} );

  return;
}

sub t_reconnect {
  my $redis = shift;

  my $reconn_state = $redis->reconnect;
  is( $reconn_state, 1, q{get current reconnection mode state} );

  $redis->reconnect(undef);
  is( $redis->reconnect, undef, q{disable reconnection mode} );

  $redis->reconnect(1);
  is( $redis->reconnect, 1, q{enable reconnection mode} );

  return;
}

sub t_min_reconnect_interval {
  my $redis = shift;

  my $t_min_reconnect_interval = $redis->min_reconnect_interval;
  is( $t_min_reconnect_interval, 5, q{get "min_reconnect_interval"} );

  $redis->min_reconnect_interval(undef);
  is( $redis->min_reconnect_interval, undef,
      q{disable "min_reconnect_interval"} );

  $redis->min_reconnect_interval(10);
  is( $redis->min_reconnect_interval, 10, q{set "min_reconnect_interval"} );

  return;
}

sub t_utf8 {
  my $redis = shift;

  my $utf8_state = $redis->utf8;
  is( $utf8_state, 1, q{get current UTF8 mode state} );

  $redis->utf8(undef);
  is( $redis->utf8, undef, q{disable UTF8 mode} );

  $redis->utf8(1);
  is( $redis->utf8, 1, q{enable UTF8 mode} );

  return;
}

sub t_on_connect {
  my $redis = shift;

  my $on_conn = $redis->on_connect;
  is( $on_conn->(), 1, q{get "on_connect" callback} );

  $redis->on_connect(undef);
  is( $redis->on_connect, undef, q{disable "on_connect" callback} );

  $redis->on_connect(
    sub {
      return 5;
    }
  );
  is( $redis->on_connect->(), 5, q{set "on_connect" callback} );

  return;
}

sub t_on_disconnect {
  my $redis = shift;

  my $on_disconn = $redis->on_disconnect;
  is( $on_disconn->(), 2, q{get "on_disconnect" callback} );

  $redis->on_disconnect(undef);
  is( $redis->on_disconnect, undef, q{disable "on_disconnect" callback} );

  $redis->on_disconnect(
    sub {
      return 6;
    }
  );
  is( $redis->on_disconnect->(), 6, q{set "on_disconnect" callback} );

  return;
}

sub t_on_connect_error {
  my $redis = shift;

  my $on_conn_error = $redis->on_connect_error;
  is( $on_conn_error->(), 3, q{get "on_connect_error" callback} );

  $redis->on_connect_error(undef);
  is( $redis->on_connect_error, undef,
      q{disable "on_connect_error" callback} );

  $redis->on_connect_error(
    sub {
      return 7;
    }
  );
  is( $redis->on_connect_error->(), 7, q{set "on_connect_error" callback} );

  return;
}

sub t_on_error {
  my $redis = shift;

  my $on_error = $redis->on_error;
  is( $on_error->(), 4, q{get "on_error" callback} );

  local %SIG;
  my $t_err;
  $SIG{__WARN__} = sub {
    $t_err = shift;
    chomp($t_err);
  };

  $redis->on_error(undef);
  
  my $err = AnyEvent::RipeRedis::Error->new( 'Some error', E_OPRN_ERROR );
  $redis->on_error->($err);

  is( $t_err, 'Some error', q{reset to default "on_error" callback} );

  $redis->on_error(
    sub {
      return 8;
    }
  );

  is( $redis->on_error->(), 8, q{set "on_error" callback} );

  return;
}

sub t_database {
  my $redis = shift;

  my $t_db_index = $redis->selected_database;

  is( $t_db_index, 0, 'get database index' );

  return;
}
