package AnyEvent::RipeRedis::Error;

use 5.008000;
use strict;
use warnings;

our $VERSION = '0.12';


sub new {
  my $class    = shift;
  my $err_msg  = shift;
  my $err_code = shift;

  my $self = bless {}, $class;

  $self->{message} = $err_msg;
  $self->{code}    = $err_code;

  return $self;
}

sub message {
  my $self = shift;
  return $self->{message};
}

sub code {
  my $self = shift;
  return $self->{code};
}

1;
__END__

=head1 NAME

AnyEvent::RipeRedis::Error - Class of error for AnyEvent::RipeRedis

=head1 DESCRIPTION

Class of error for L<AnyEvent::RipeRedis>. Objects of this class can be passed
to callbacks.

=head1 CONSTRUCTOR

=head2 new( $err_msg, $err_code )

Creates error object.

=head1 METHODS

=head2 message()

Get error message.

=head2 code()

Get error code.

=head1 SEE ALSO

L<AnyEvent::RipeRedis>

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2012-2016, Eugene Ponizovsky, E<lt>ponizovsky@gmail.comE<gt>.
All rights reserved.

This module is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut
