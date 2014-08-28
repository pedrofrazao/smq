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
# push messages
#
for my $i (1..3) {
  $mq->push( q{m_}.$i );
  is( $mq->size(), ( $q_size + $i ), q{queue size }.( $q_size + $i ) );
  $test_count++;
}

#
# pop messages
#
my $m = $mq->pop();
is( $m, q{m_1}, q{1st message assigned} );
$test_count++;

for my $i (1..5) {
  my $m = $mq->pop();
  is( $m, undef, q{no message poped, 1 assigned} );
  $test_count++;
  sleep( POP_TIMEOUT + 1 );
  $m = $mq->pop();
  is( $m, q{m_1}, $m .q{ message poped after time out} );
  $test_count++;
  is( $mq->message_retries_count( q{m_1} ), $i, qq{$m retry count $i} );
  $test_count++;
}

sleep( POP_TIMEOUT + 1 );
$m = $mq->pop();
is( $m, q{m_2}, $m. q{ message assigned} );
$test_count++;

is( $mq->done( $m, q{ok} ), 1, $m .q{ done} );
$test_count++;

$m = $mq->pop();
is( $m, q{m_3}, $m .q{ poped} );
$test_count++;

done_testing( $test_count );
