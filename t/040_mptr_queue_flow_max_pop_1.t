#!/bin/env perl

use warnings;
use strict;
use Test::More;
use FindBin;

use lib $FindBin::Bin .q{/../};

my $test_count = 1;

use_ok( q{Redismq::MaxPopTRQueue} );

my $mq = Redismq::MaxPopTRQueue->new( { name => q{mq_test},
					pop_timeout => 10,
					max_retries => 6,
					max_pop => 1,
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
# add 5 messages - push message
#
for my $i (1..5) {
  my $message = q{m}.$i.q{_};
  $mq->push( $message );
  is( $mq->size(), ( $q_size + $i ), qq{'$message' added -> queue size }
      .( $q_size + $i ) );
  $test_count++;
}

#
# pop message
#
{
  my $m = $mq->pop();
  ok( $m =~ m{\A m1_}smx, q{got message }.$m);
  $test_count++;
  is( $mq->size(), 4, q{4 messages on the queue} );
  $test_count++;
  is( $mq->assigned_size(), 1, q{1 message on the assigned queue} );
  $test_count++;

  $m = $mq->pop();
  is( $m, undef, q{can't get a 2nd message, 1 already poped});
  $test_count++;
  is( $mq->size(), 4, q{4 messages on the queue} );
  $test_count++;
  is( $mq->assigned_size(), 1, q{1 message on the assigned queue} );
  $test_count++;

  # sleep for the timeout
  sleep( $mq->pop_timeout()+1 );

  ok(defined($m = $mq->pop()), qq{got message '$m' after }
     . $mq->pop_timeout() .q{s} );
  $test_count++;
  ok( $m =~ m{\A m2_}smx, q{got message after the 1st messaged timedout }.$m);
  $test_count++;
  is( $mq->size(), 4, q{4 messages on the queue} );
  $test_count++;
  is( $mq->assigned_size(), 1, q{1 message on the assigned queue} );
  $test_count++;

  is( $mq->drop( $m ), 1, qq{$m dropped});
  $test_count++;
  ok(defined($m = $mq->pop()), q{got message after drop} );
  $test_count++;
  is( $mq->size(), 3, q{3 messages on the queue} );
  $test_count++;
  is( $mq->assigned_size(), 1, q{1 message on the assigned queue} );
  $test_count++;

  ok( ! defined( $mq->pop()), q{no message});
  $test_count++;

  $mq->done($m, $m.q{result});

  ok(defined($m = $mq->pop()), q{got a new message after done of previous} );
  $test_count++;
  is( $mq->size(), 2, q{2 messages on the queue} );
  $test_count++;
  is( $mq->assigned_size(), 1, q{1 message on the assigned queue} );
  $test_count++;

  ok( ! defined( $m = $mq->pop()), q{no message});
  $test_count++;
}

done_testing( $test_count );
