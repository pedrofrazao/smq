package Redismq::RedisScript::Pop;

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
-- print("Rmq::Pop: start: ".. now .." : ".. retry_penalty .." : ".. (now+retry_penalty))
local assigned_timeout= redis.call('zrangebyscore', zset_assigned, 0, now+0,'withscores','limit',0,1)
if assigned_timeout ~= false and #assigned_timeout ~= 0 then
  print("Rmq::Pop: move message (".. assigned_timeout[1] ..") after time out in " .. (now - assigned_timeout[2]) )
  redis.call('zrem',zset_assigned, assigned_timeout[1])
  redis.call('zadd',zset, (now + retry_penalty),  assigned_timeout[1])
  redis.call('incr',zset_assigned .."_retries_count_"..assigned_timeout[1])
  redis.call('expire', assigned_timeout[1].."_retries_count", message_timeout)
end

local message = redis.call('zrangebyscore', zset, 0,now+0,'limit',0,1)
-- print( "Rmq::Pop: message " .. #message )
if message ~= false and #message ~= 0 then
  redis.call('zrem',zset,message[1])
  redis.call('zadd',zset_assigned,( now + pop_timeout ),message[1])
  print( "Rmq::Pop: " .. zset_assigned.." with a new member ".. message[1] .." will timeout within "..pop_timeout )
--   print( "Rmq::Pop: end: ".. message[1])
  return message[1]
end
-- print( "Rmq::Pop: end: undef" )
return
EOCB
     );

1;

=head1 NAME

Redismq::RedisScript::Pop - Pop action from the queue

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
