package Redismq::MaxPopTRQueue;

use strict;
use warnings;
use Time::HiRes qw/time/;

use constant POP_LOCK_KEY_PREFIX => q{__MaxPopTRQueue_pop_lock_};
use constant POP_LOCK_KEY_EXPIRE => 5;

use Redismq::RedisScript::Reassign;
use Moo;

extends 'Redismq::TimedRetryQueue';

has 'max_pop'
  => (
      is => 'ro',
      default => 0,
     );

has 'pop_lock_key'
  => (
      is => 'rwp',
      default => sub { POP_LOCK_KEY_PREFIX . $_[0]->name; },
     );

has '__pop_lock_value'
  => (
      is => 'rwp',
     );

has 'reassign_s'
  => (
      is => 'lazy',
      default => sub { Redismq::RedisScript::Reassign->new(
							   redis => $_[0]->redis
							  );
		     },
     );

has 'unassign'
  => (
      is => 'ro',
      default => 1,
     );

around pop => \&__pop;

sub reassign {
  my $self = shift;
  my $res = $self->reassign_s()->eval_rs
    (
     # keys
     [
       $self->name_assigned,
     ],
     # argv
     [ time(),
       ($self->max_retries * $self->pop_timeout),
     ]);

  return $res;
}

sub __pop {
  my $orig = shift;
  my $self = shift;

  return
    unless $self->_lock_pop_access();

  my $res = __pop_after_lock( $orig, $self, @_ );

  $self->_unlock_pop_access();

  return $res;
}

sub __pop_after_lock {
  my $orig = shift;
  my $self = shift;

  my $queue_size = $self->assigned_size();

  if( $queue_size < $self->max_pop ) {
    #
    # assigned queue not full
    #
    my $message = $self->$orig(@_);
    return $message;
  } else {
    #
    # try to get space on the assigned queue
    #
    my $message = $self->reassign();
    return $message;
  }
}

sub _lock_pop_access {
  my $self = shift;

  my $lock_value = $$ .q{_}. time();
  my $r = $self->redis()->setnx( $self->pop_lock_key, $lock_value );
  # no lock!
  return 0 unless $r;

  # got the lock
  $self->redis()->expire( $self->pop_lock_key, POP_LOCK_KEY_EXPIRE );
  $self->_set___pop_lock_value( $lock_value );
  return 1;
}

sub _unlock_pop_access {
  my $self = shift;

  my $r = $self->redis()->get( $self->pop_lock_key );
  die( qq{lock used by $r\n} )
    if( $r ne $self->__pop_lock_value );

  $r = $self->redis()->del( $self->pop_lock_key );

  return 1 if $r == 1;

  die( qq{lock del with result of $r\n} );
}

1;
