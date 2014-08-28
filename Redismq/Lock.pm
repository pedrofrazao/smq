package Redismq::Lock;

use strict;
use warnings;

use Time::HiRes qw/time/;

use Moo;

extends 'Redismq';

has 'name'
  => (
      is => 'ro',
      required => 1,
     );

has 'timeout'
  => (
      is => 'ro',
      default => 10,
     );

has '__internal_name'
  => (
      is => 'lazy',
      default => \&__internal_name_init,
     );

sub __internal_name_init {
  my $self = shift;
  return __PACKAGE__ .q{_}. $self->name;
}


sub lock {
  my $self = shift;
  if( $self->redis()->setnx( $self->__internal_name, $$ ) ) {
    if( $self->redis()->expire( $self->__internal_name, $self->timeout ) ) {
      return 1;
    } else {
      $self->redis()->del( $self->__internal_name );
    }
  }
  return 0;
}

sub unlock {
  my $self = shift;
  my $lock = $self->redis()->get( $self->__internal_name );

  return 1
    unless defined( $lock );

  if( $lock eq $$ ) {
    if( $self->redis()->del( $self->__internal_name ) ) {
      return 1;
    }
  }
  return 0;
}

1;

=head1 NAME

=head1 AUTHOR

pedro.frazao

=cut
