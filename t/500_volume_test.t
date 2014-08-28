#!/bin/env perl

use warnings;
use strict;
use Test::More;
use FindBin;

use lib $FindBin::Bin .q{/../};

my $test_count = 1;

use_ok( q{Redismq::TimedRetryQueue} );

my $mq = Redismq::TimedRetryQueue->new( { name => q{mq_test},
					  pop_timeout => 10,
					  max_retries => 6,
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
# push messages
#
for my $i (1..10_000) {
  $mq->push( q{m_}.$i);
  is( $mq->size(), ( $q_size + $i ), q{queue size }.( $q_size + $i ) );
  $test_count++;
}

while(my $m = $mq->pop()) {
  ok( defined $m, qq{no message $m} );
  is( $mq->done($m, q{ok}),1, qq{messaged marked done} );
  $test_count+=2;
}

is( $mq->size(), 0, q{empty queue});
$test_count++;

done_testing( $test_count );
