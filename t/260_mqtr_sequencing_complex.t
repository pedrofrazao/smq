#!/bin/env perl

use warnings;
use strict;
use Test::More;
use FindBin;
use Time::HiRes qw/time sleep/;

use lib $FindBin::Bin .q{/../};

my $test_count = 1;

use_ok( q{Redismq::StrictSequenceTRQueue} );

use constant POP_TIMEOUT => 4;

my @processes = ();
my $mq = Redismq::StrictSequenceTRQueue->new( { name => q{mq_test},
						pop_timeout => POP_TIMEOUT,
						max_retries => 3,
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
# start all consumers
#
sub worker {
  my ($mq, $func) = @_;

  if( my $pid = fork ) {
    push( @processes, $pid );
    return;
  }

  while(1) {
    $func->();
    sleep(0.5);
  }
}

sub worker_func {
  my ($sleep_time ) = @_;
  my $m = $mq->pop();
  return unless $m;

  my ( $m_id ) = $m =~ m{\A m _ (\d+) \Z}smx;

  my $s = time();
  sleep( POP_TIMEOUT *3  ) if( $m_id == 2 );
  sleep( $sleep_time );
  my $delta = time()-$s;
  my $res = $mq->done($m,qq{done after $delta});
  if( $res ) {
    $mq->redis()->rpush($0, qq{done $m after $delta});
  } else {
    $mq->redis()->rpush($0, qq{drop $m after $delta});
  }
  exit if $m eq q{end};
}

worker( $mq, sub{ worker_func( POP_TIMEOUT / 4 ) } ) for (1..40);
worker( $mq, sub{ worker_func( POP_TIMEOUT * 2 ) } ) for (1..1);


#
# sent some messages
#
sleep(1);
for my $i (1..10) {
  $mq->push( q{m_}.$i );
}

sleep(30);
kill(1, @processes);

LOOP_LOG:
while( my $log = $mq->redis()->lpop( $0 ) ) {
  my ( $res, $message, $time )
    = $log =~ m{\A (\w+) \s (\w+) \s after \s ([\d\.]+ \Z) }smx;
  my $result = $mq->get_result( $message );

  if( $message eq q{m_2} ) {
    is( $result, undef, qq{m_2 drop result after max retries reached} );
    $test_count++;
    is( $res, q{drop}, q{m_2 droped} );
    $test_count++;
    next LOOP_LOG;
  }

  my $expected_result = q{done after }. $time;
  if( $res eq q{done} ) {
    is( $result, $expected_result, qq{get_result $message: }. $result)
      or diag( $log );
    $test_count++;
  } else {
    ok( $result ne $expected_result, qq{get_result $message: droped after $time} )
      or diag( $log );
    $test_count++;
  }
}

done_testing( $test_count );
