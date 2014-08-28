package Redismq::RedisScript;

use strict;
use warnings;

use Moo;

extends 'Redismq::RedisScript::Eval';

has 'code'
  => (
      is => 'ro',
      required => 1,
     );

has 'redis'
  => (
      is => 'ro',
      required => 1,
     );


1;
