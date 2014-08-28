#!/bin/env perl

use warnings;
use strict;
use Test::More;
use FindBin;
use Time::HiRes;

use lib $FindBin::Bin;
use lib $FindBin::Bin .q{/../};

use test_with_n_workers;

my $test_count = 1;

use_ok( q{Redismq::TimedRetryQueue} );

my $mq = Redismq::TimedRetryQueue->new( { name => q{mq_test},
					  pop_timeout => 10,
					  max_retries => 6,
					} );
my $qsize = 10_000;

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
for my $i (1.. $qsize) {
  $mq->push( q{m_}.$i);
}
is( $mq->size(), ($qsize), q{queue size }. $qsize);
$test_count++;


$test_count += run_with(
			mq => $mq,
			n => 1,
			sleep => 500,
		       );

is( $mq->size(), 0, q{empty queue});
$test_count++;

done_testing( $test_count );
