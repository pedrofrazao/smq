package Redismq::RedisScript::PopSS;

use strict;
use warnings;

use Moo;
extends 'Redismq::RedisScript';

has '+code'
  => (
      is => 'ro',
      default => <<EOCB,
local zset = KEYS[1]
local zset_assigned = KEYS[2]
local now = ARGV[1]
local pop_timeout = ARGV[2]
local message_timeout = ARGV[3]
local retry_penalty = ARGV[4]
local assigned_timeout = redis.call('zrange', zset_assigned, 0, -1,'withscores')
if assigned_timeout ~= false and #assigned_timeout ~= 0 then
  if assigned_timeout[2] > now then
--[[
    print("Rmq::PopSS: 1 assigned (".. assigned_timeout[1] ..") time out in ".. (assigned_timeout[2] - now ) )
]]
    return
  end
  print("Rmq::PopSS: move message (".. assigned_timeout[1] ..") after timeout at " .. (now - assigned_timeout[2]) )
  redis.call('zadd',zset_assigned, (now + retry_penalty + pop_timeout), assigned_timeout[1])
  redis.call('incr',zset_assigned .."_retries_count_"..assigned_timeout[1])
  redis.call('expire', assigned_timeout[1].."_retries_count", message_timeout)
  return assigned_timeout[1]
end
local message = redis.call('zrangebyscore', zset, 0,now+0,'limit',0,1)
if message ~= false and #message ~= 0 then
  redis.call('zrem',zset,message[1])
  redis.call('zadd',zset_assigned, now+pop_timeout,message[1])
  print( "Rmq::PopSS: ".. zset_assigned.." with a new member ".. message[1] .." will timeout at ".. pop_timeout )
  return message[1]
end
return
EOCB
     );

1;

=head1 NAME

Redismq::RedisScript::PopSS - Pop action from the queue maintaining a strict sequence policy

=head1 ARGS

=head2 zset name

name of the main queue

=head2 zset_assigned

name of the queue for the assigned messages

=head2 now

current time (unix time stamp).

=head2 pop_timeout

time out for the interval between pop and done actions in sec.

=head2 message_timeout

the message time out in sec.

=head2 retry_penalty

penalty for retry

=head1 AUTHOR

pedro.frazao

=cut
