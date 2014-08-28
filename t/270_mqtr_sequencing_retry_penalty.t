#!/bin/env perl

use warnings;
use strict;
use Test::More;
use FindBin;
use Time::HiRes qw/time sleep/;

use lib $FindBin::Bin .q{/../};

my $test_count = 1;

use_ok( q{Redismq::StrictSequenceTRQueue} );

use constant POP_TIMEOUT => 1;

my @processes = ();
my $mq = Redismq::StrictSequenceTRQueue->new( { name => q{mq_test},
						pop_timeout => POP_TIMEOUT,
						max_retries => 4,
						retry_penalty => 5,
					      } );

#
# clean db
#
$mq->redis->FLUSHALL();
is( $mq->redis->keys( '*' ), 0, q{empty db} );
$test_count++;
my $q_size = $mq->size();
my $q_ass_size = $mq->assigned_size();

our $s_time = time();

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
  $mq->redis()->publish(q{debug}, qq{start $m }.(time() - $s_time));

  my $s = time();
  if( $m eq q{m_2} ) {
    $mq->redis()->publish( q{debug}, qq{sleep $m }.(time() - $s_time) );
    sleep( POP_TIMEOUT * 40  );
  }
  sleep( $sleep_time );
  my $delta = time()-$s;
  my $res = $mq->done($m,qq{done after $delta at }.(time() - $s_time));
  if( $res ) {
    $mq->redis()->rpush($0, qq{done $m after $delta at }.(time() - $s_time));
  } else {
    $mq->redis()->rpush($0, qq{drop $m after $delta at }.(time() - $s_time));
  }
  $mq->redis()->publish(q{debug}, qq{end $m }.(time() - $s_time));
  exit if $m eq q{end};
}

worker( $mq, sub{ worker_func( POP_TIMEOUT / 4 ) } ) for (1..7);
#worker( $mq, sub{ worker_func( POP_TIMEOUT * 2 ) } ) for (1..1);


#
# sent some messages
#
sleep(1);
for my $i (1..10) {
  $mq->push( q{m_}.$i );
}

sleep(45);
kill(1, @processes);

LOOP_LOG:
while( my $log = $mq->redis()->lpop( $0 ) ) {

  my ( $res, $message, $time, $at )
    = $log =~ m{\A (\w+) \s (\w+) \s after \s ([\d\.]+) \s at \s ([\d\.]+) \Z }smx;
  my $result = $mq->get_result( $message );
  my $expected_result = q{done \s after \s}. $time;

  if( $message eq q{m_2} ) {
    is( $result, undef, q{no result for }. $message .q{ }. $time );
    $test_count++;
  } elsif( $res eq q{done} ) {
    ok( $result =~ m{\A $expected_result}smx, qq{$message done with: }. $result );
    $test_count++;
  } elsif( $res eq q{drop} ) {
    is( $result, undef, qq{no result for $message} );
    $test_count++;
  } else {
    BAIL_OUT( qq{unexpected result for $message with $result} );
  }
}

done_testing( $test_count );
