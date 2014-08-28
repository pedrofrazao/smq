package Redismq::RedisScript::Reassign;

use strict;
use warnings;

use Moo;
extends 'Redismq::RedisScript';

has '+code'
  => (
      is => 'ro',
      default => <<EOCB,
local zset_assigned = KEYS[2]
local now = ARGV[1]
local message_timeout = ARGV[2]

local message_timedout = redis.call('zrangebyscore', zset_assigned, 0, now,'limit',0,1)
if message_timedout ~= false and #message_timedout ~= 0 then
  print("reassign " .. #message_timedout .." messages after timeout " .. message_timedout[1])
  redis.call('zadd',zset_assigned,message_timeout, message_timedout[1])
  redis.call('incr',message_timedout[1].."_retries_count")
  redis.call('expire', message_timedout[1].."_retries_count", message_timeout)
  return message[1]
end
return
EOCB
     );

1;
