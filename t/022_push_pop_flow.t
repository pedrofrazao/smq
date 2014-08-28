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
my $message;


#
# clean db
#
$mq->redis->FLUSHALL();
is( $mq->redis->keys( '*' ), 0, q{empty db} );
$test_count++;
my $q_size = $mq->size();
my $q_ass_size = $mq->assigned_size();

#
# push 10 messages
#
for my $i (1..10) {
  $mq->push( q{m}.$i.q{_} );
  is( $mq->size(), ( $q_size + $i ), q{queue size }.( $q_size + $i ) );
  $test_count++;
}

is( $mq->assigned_size(), $q_ass_size, q{assigned queue size unchanged} );
$test_count++;

$q_size = $mq->size();

#
# pop all the 10 messages
#
for my $i (1..10) {
  my $m = $mq->pop();
  ok( $m =~ m{\A m $i _}smx, q{got message }.$m);
  $test_count++;
  is( $mq->size(), ( $q_size - $i ), q{queue size after pop} );
  $test_count++;
  is( $mq->assigned_size(), ($q_ass_size + $i), q{assigned_size queue size after pop} );
  $test_count++;
  my @r = $mq->redis()->zrange( q{mq_test_assigned}, -1,-1, q{withscores});
  ok( $r[0] =~ m{\A m $i _}smx, q{message on the assigned queue }. $r[0] );
  $test_count++;
  ok( $r[1] <= time() + $mq->pop_timeout() +1,
      q{timeout for }. $r[0]. q{ at }. $r[1] )
    or diag( qq{got timeout of $r[1], (}. ( $r[1] - time() ) .q{ from now} );
  $test_count++;
}

# wait for the timeout
sleep( $mq->pop_timeout +1);

#
# get the first 5 timedout messages
#
for my $i (1..5) {
  my $m = $mq->pop();
  ok( $m =~ m{\A m $i _}smx, q{got message }.$m.q{ after timeout});
  $test_count++;
}

#
# remote 5 of the messages from the queue
#
LOOP_I:
for my $i (1..10) {
  next LOOP_I unless( $i % 2 );
  ok($mq->drop( q{m}.$i.q{_} ), q{drop of }. q{m}.$i.q{_});;
  $test_count++;
}

#
# test the queue size after the remove
#
is( $mq->assigned_size(), 5, q{5 left in the assigned queue} );
$test_count++;
is( $mq->size(), 0, q{0 left in the queue} );
$test_count++;

#
# done and result
#
LOOP_I2:
for my $i (1..10) {
  next LOOP_I2 if( $i % 2 );
  my $message = q{m}.$i.q{_};
  is($mq->done( q{m}.$i.q{_}, $message.q{result} ),1 , qq{done for $message});
  $test_count++;
}

#
# get the result for all the 10 messages
#
for my $i (1..10) {
  my $message = q{m}.$i.q{_};
  my $res = $mq->get_result( $message );
  if( ! ($i % 2 )) {
    is( $res, $message.q{result}, q{got result for }. $message);
    $test_count++;
  } else {
    is( $res, undef, q{no result for the removed message }. $message );
    $test_count++;
  }
}

done_testing( $test_count );
