#!/bin/env perl

use warnings;
use strict;
use Test::More;
use FindBin;
use Time::HiRes qw/time sleep/;

use lib $FindBin::Bin .q{/../};

my $test_count = 1;

use_ok( q{Redismq::StrictSequenceTRQueue} );

use constant POP_TIMEOUT => 3;

my $mq = Redismq::StrictSequenceTRQueue->new( { name => q{mq_test},
						pop_timeout => POP_TIMEOUT,
						max_retries => 5,
					      } );

#
# clean db
#
$mq->redis->FLUSHALL();
is( $mq->redis->keys( '*' ), 0, q{empty db} );
$test_count++;
my $q_size = $mq->size();
my $q_ass_size = $mq->assigned_size();

#
# pop messages
#
for(1..10) {
  my $m = $mq->pop();
  is( $m, undef, q{no message found} );
  $test_count++;
  sleep(1);
}

done_testing( $test_count );
