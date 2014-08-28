#!/bin/env perl

use warnings;
use strict;
use Test::More;
use FindBin;
use Time::HiRes qw/time sleep/;

use lib $FindBin::Bin .q{/../};

my $test_count = 1;

use_ok( q{Redismq::TimedRetryQueue} );

my $mq = Redismq::TimedRetryQueue->new( { name => q{mq_test},
					  pop_timeout => 1,
					  max_retries => 6,
					} );
my $message = q{123};


#
# clean db and init of control vars
#
$mq->redis->FLUSHALL();
is( $mq->redis->keys( '*' ), 0, q{empty db} );
$test_count++;
my $q_size = $mq->size();
my $q_ass_size = $mq->assigned_size();

#
# add message to the queue - push message
#
$mq->push( $message );
is( $mq->size(), ( $q_size + 1 ), q{queue size} );
$test_count++;

is( $mq->assigned_size(), $q_ass_size, q{assigned queue size unchanged} );
$test_count++;

#
# remove message from the queue - pop message
#
my $m = $mq->pop();
is( $m, $message, q{got message});
$test_count++;
is( $mq->size(), $q_size, q{queue size after pop} );
$test_count++;
is( $mq->assigned_size(), ($q_ass_size + 1), q{assigned_size queue size after pop} );
$test_count++;

#
# pop without any message on the queue
#
$m = $mq->pop();
is( $mq->pop(), undef, q{no message});
$test_count++;
is( $mq->size(), $q_size, q{queue size after pop} );
$test_count++;
is( $mq->assigned_size(), ($q_ass_size + 1), q{assigned_size queue size after pop} );
$test_count++;

#
# get the same message from the queue after timeout
#
for my $k (1..6) {
  #
  # wait for the timeout
  #
  pass( qq{$k - sleep for }. $mq->pop_timeout *1 .q{sec} );
  $test_count++;
  sleep( $mq->pop_timeout + 1);

  #
  # pop after pop_timeout
  #
  my  $m = $mq->pop();
  is( $m, $message, q{got message});
  $test_count++;
  is( $mq->size(), $q_size, qq{$k - queue size after pop} );
  $test_count++;
  is( $mq->assigned_size(), ($q_ass_size + 1), qq{$k - assigned_size queue size after pop} );
  $test_count++;
  is( $mq->message_retries_count( $m ), $k, qq{$k - $m - retries count with }.$k);
  $test_count++;
}

# diag( q{sleep for }. $mq->pop_timeout .q{sec});
sleep( $mq->pop_timeout );
#
# try to get the massage from the queue after the last timeout
# pop without any message on the queue
#
$m = $mq->pop();
is( $mq->pop(), undef, q{no message});
$test_count++;
is( $mq->size(), $q_size, q{queue size after pop} );
$test_count++;
is( $mq->assigned_size(), ($q_ass_size), q{assigned_size queue size after pop} );
$test_count++;

done_testing( $test_count );
