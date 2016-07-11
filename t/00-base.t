use 5.008000;
use strict;
use warnings;

use Test::More tests => 8;

my $T_CLIENT_CLASS;
my $T_ERR_CLASS;

BEGIN {
  $T_CLIENT_CLASS = 'AnyEvent::RipeRedis';
  use_ok( $T_CLIENT_CLASS );

  $T_ERR_CLASS = 'AnyEvent::RipeRedis::Error';
  use_ok( $T_ERR_CLASS );
}

can_ok( $T_CLIENT_CLASS, 'new' );
my $redis = new_ok( $T_CLIENT_CLASS );

can_ok( $T_ERR_CLASS, 'new' );
my $err = new_ok( $T_ERR_CLASS => [ 'Some error', 9 ] );

can_ok( $err, 'message' );
can_ok( $err, 'code' );
