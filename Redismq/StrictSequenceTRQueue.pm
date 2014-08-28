package Redismq::StrictSequenceTRQueue;

use strict;
use warnings;

use Moo;
use Redismq::RedisScript::PopSS;

extends 'Redismq::TimedRetryQueue';

has '+pop_s'
  => (
      is => 'lazy',
      default => sub { Redismq::RedisScript::PopSS->new(
							redis => $_[0]->redis
						       );
		     },
     );
1;


=head1 NAME

Redismq::StrictSequenceTRQueue - message queue forcing a strict sequence of messages with time out and retry capabilities

=head1 DESCRIPTION

message queue system based on redis, with time out and retry capabilities and strict sequence message delivery

=head1 AUTHOR

pedro.frazao

=cut
