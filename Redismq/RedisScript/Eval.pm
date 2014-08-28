package Redismq::RedisScript::Eval;

use strict;
use warnings;

use Moo;

has '_sha'
  => (
      is => 'lazy',
      default => \&__sha,
     );

sub eval_rs {
  my $self = shift;
  my ($keys, $args) = @_;
  return $self->redis()->evalsha
    ( $self->_sha(), scalar(@$keys), @$keys, @$args );
}

sub __sha {
  my $self = shift;
  return $self->redis()->script_load( $self->code );
}

1;
