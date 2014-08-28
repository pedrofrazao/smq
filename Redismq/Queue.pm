package Redismq::Queue;

use strict;
use warnings;
use Sys::Hostname;

use Moo;

extends 'Redismq';

#
# lua script
#
use constant POP_M => <<EOLC;
local zset = KEYS[1]
local zset_assigned = KEYS[2]
local now = ARGV[1]
local pop_timeout = ARGV[2]
local object_timeout = ARGV[3]
local message_timedout = redis.call('zrangebyscore', zset_assigned, 0, now,'limit',0,1)
if message_timedout ~= false and #message_timedout ~= 0 then
  print("move " .. #message_timedout .." messages after timeout " .. message_timedout[1])
  redis.call('zrem',zset_assigned, message_timedout[1])
  redis.call('zadd',zset,now, message_timedout[1])
  redis.call('incr',message_timedout[1].."_retries_count")
  redis.call('expire', message_timedout[1].."_retries_count", object_timeout)
end
local message = redis.call('zrangebyscore', zset, 0,now,'limit',0,1)
if message ~= false and #message ~= 0 then
  redis.call('zrem',zset,message[1])
  redis.call('zadd',zset_assigned,pop_timeout,message[1])
  print( zset_assigned.." with a new member ".. message[1] .." will timeout at "..pop_timeout )
  return message[1]
end
return
EOLC

use constant PUSH_M => <<EOLC;
local object_key = KEYS[1]
local zset = KEYS[2]
local score = ARGV[1]

redis.call('ZADD', zset, score, object_key)
return
EOLC


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

has 'pop_sha'
  => (
      is => 'rwp',
      lazy => 1,
      default => \&__pop_sha,
     );

has 'push_sha'
  => (
      is => 'rwp',
      lazy => 1,
      default => \&__push_sha,
     );

has 'message_id_prefix'
  => (
      is => 'rwp',
      lazy => 1,
      default => sub { q{rmq_q_}.$_[0]->name .q{_}.hostname().q{_}.$$.q{_}},
     );

has 'message_id'
  => (
      is => 'rwp',
      default => 0,
     );

has 'message_timeout'
  => (
      is => 'ro',
      default => 3_600_000,
     );

has 'pop_timeout'
  => (
      is => 'ro',
      default => 300_000,
     );

sub push {
  my $self = shift;
  my ( $message ) = @_;

  my $m_id = $self->gen_message_id( $message );
  $self->redis()->setex( $m_id, $self->message_timeout(), $message);
  $self->redis()->evalsha( $self->push_sha(),
			   2,
			   $m_id,
			   $self->name,
			   time(),
			 );
  return $m_id;
}

sub gen_message_id {
  my $self = shift;
  my $m_id = $self->message_id();
  $self->_set_message_id( $m_id + 1 );
  return $self->message_id_prefix() . $m_id;
}

sub pop {
  my $self = shift;

  my $m_id = $self->redis()->evalsha( $self->pop_sha(),
				      2,
				      $self->name,
				      $self->name_assigned,
				      time(),
				      time() + $self->pop_timeout,
				      time() + $self->message_timeout,
				    );
  return(undef, undef)
    unless( defined $m_id );
  my $message = $self->redis()->get( $m_id );

  # !!! TODO TODO TODO TODO
  # !!! drop entry on the queue if `get` return undef
  #

  return($m_id, $message);

  #my $m = $self->redis()->zrange( $self->name, 0, 1 );
  #$self->redis()->zrem( $self->name, $m->[0] );
  # return $m;
}

sub size {
  my $self = shift;
  return $self->redis()->zcard( $self->name );
}

sub assigned_size {
  my $self = shift;
  return $self->redis()->zcard( $self->name_assigned );
}

sub __pop_sha {
  my $self = shift;

  return $self->redis()->script_load( POP_M );
}

sub __push_sha {
  my $self = shift;

  return $self->redis()->script_load( PUSH_M );
}

sub message_retries_count {
  my $self = shift;
  my ($m_id) = @_;

  my $retries_count = $self->redis->get( $m_id .q{_retries_count} );
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

1;
