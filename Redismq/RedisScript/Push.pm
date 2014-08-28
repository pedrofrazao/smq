package Redismq::RedisScript::Push;

use strict;
use warnings;

use Moo;
extends 'Redismq::RedisScript';

has '+code'
  => (
      is => 'ro',
      default => <<EOCB,
local zset = KEYS[1]
local score = ARGV[1]
local message = ARGV[2]

redis.call('ZADD', zset, score, message)
return
EOCB
     );

1;
