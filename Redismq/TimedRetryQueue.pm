package Redismq::TimedRetryQueue;

use strict;
use warnings;

use Sys::Hostname;
use Time::HiRes qw/time/;

use Moo;
use Redismq::RedisScript::Pop;

extends 'Redismq';

has 'name'
  => (
      is => 'ro',
      required => 1,
     );

has 'name_assigned'
  => (
      is => 'lazy',
      default=> sub{ $_[0]->name . q{_assigned} },
     );

has 'result_prefix'
  => (
      is => 'lazy',
      default => sub{ $_[0]->name .q{_result_} },
     );

has 'pop_s'
  => (
      is => 'lazy',
      default => sub { Redismq::RedisScript::Pop->new(
						      redis => $_[0]->redis
						     );
		     },
     );

has 'pop_timeout'
  => (
      is => 'ro',
      default => 600,
     );

has 'max_retries'
  => (
      is => 'ro',
      default => 5,
     );

has 'result_timeout'
  => (
      is => 'ro',
      default => 3600,
     );

has 'retry_penalty'
  => (
      is => 'ro',
      default => 0,
     );

sub size {
  my $self = shift;
  return $self->redis()->zcard( $self->name );
}

sub assigned_size {
  my $self = shift;
  return $self->redis()->zcard( $self->name_assigned );
}

sub message_retries_count {
  my $self = shift;
  my ($message) = @_;

  my $retries_count = $self->redis->get( $self->name_assigned(). q{_retries_count_} .$message );
  return ( $retries_count ? $retries_count : 0 );
}

sub drop {
  my $self = shift;
  my ($m_id) = @_;

  # try to remove from assigned queue
  my $c = $self->redis()->zrem( $self->name_assigned, $m_id );
  return $c if $c;

  # try to remove from main queue
  $c = $self->redis()->zrem( $self->name, $m_id );
  return $c;
}

sub done {
  my $self = shift;
  my ($message, $result) = @_;

  my $c = $self->drop( $message );

  #
  # have previous result drop this
  #
  return
    if( $self->redis()->exists( $self->result_prefix . $message ) );

  #
  # max reties reached drop result
  #
  return
    if( $self->message_retries_count( $message ) >= $self->max_retries );

  $self->redis()->setex( $self->result_prefix . $message,
			 $self->result_timeout,
			 $result );
  return $c;
}

sub get_result {
  my $self = shift;
  my ($message) = @_;

  my $r = $self->redis()->get( $self->result_prefix . $message );

  return $r;
}

sub pop {
  my $self = shift;
  my $message = $self->pop_s()->eval_rs
    (
     # keys
     [ $self->name,
       $self->name_assigned,
     ],
     # argv
     [ time(),
       $self->pop_timeout,
       ($self->max_retries * ($self->retry_penalty + $self->pop_timeout) * 4),
       $self->retry_penalty,
     ]);
  return
    unless defined $message;

  my $r_count = $self->message_retries_count( $message );
  if( $r_count > $self->max_retries ) {
    # too old -> drop and get other
    $self->drop( $message );
    return $self->pop();
  }

  return $message;
}

sub push {
  my $self = shift;
  my ($message, $time) = @_;

  $time ||= time();

  return $self->redis()->zadd( $self->name, $time, $message );
}

1;

=head1 NAME

Redismq::TimedRetryQueue - message queue with time out and retry capabilities

=head1 DESCRIPTION

message queue system based on redis, with time out and retry capabilities.

=head1 METHODS

=head2 push

add a message to the queue

example:
  push(m1) -  Q[], A[] => Q[m1_t1], A[]; return 1
  push(m2) -  Q[m1], A[] => Q[m1_t1,m2_t2], A[]; return 1

=head2 pop

retrieve a message for the queue

example:
  pop() - Q[], A[] => Q[], A[]; return undef
  pop() - Q[m1], A[] => Q[], A[m1_t1]; return m1
  pop() - Q[], A[m1_t1] => Q[], A[m1_t1]; return undef
  pop() - Q[m2], A[m1_t1] => Q[], A[m1_t1,m2_t2]; return m2
  pop() - Q[m3_t3], A[m1_t1^{r0}',m2_t2] => Q[m3], A[m1_t2^{r1},m2_t2]; return m1
  pop() - Q[m2], A[m1^{r_max}'] => Q[m2], A[]; return pop()
  pop() - Q[m3], A[m1^{r_max}',m2] => Q[m3], A[m2]; return pop()

where
  m1_t2 - message 1 without reties to process at time 2
  m1^{t0} - message 1 also without retries
  m1^{t1}' - message 1 with 1 retry and time outed
  m3_t4^{t_max} - message 3 in the last retry to process at time 4
  Q[] - empty queue
  A[m1] - assigned queue with the message 1

=head1 AUTHOR

pedro.frazao

=cut
