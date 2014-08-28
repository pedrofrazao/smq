package Redismq::Queue_base;

use strict;
use warnings;

use Moo;

extends 'Redismq';

#
# lua script
#
use constant PUSH_M => <<EOLC;
eval "local val=tonumber(redis.call('get','c')); if( val<10) then val=val+1; redis.call('set','c',val); end; return redis.call('get','c');" 0


local val=redis.call('get','c');
if( val<10) then val=val+1;
EOLC

sub __pop_sha {
  my $self = shift;

  

}


1;
