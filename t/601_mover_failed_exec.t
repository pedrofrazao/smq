#!/bin/env perl

use warnings;
use strict;
use Test::More;
use FindBin;
use Time::HiRes qw/time sleep/;

use lib $FindBin::Bin .q{/../};

my $test_count = 2;

use_ok( q{Redismq::TimedRetryQueue} );
use_ok( q{Redismq::Mover} );

my $mq1 = Redismq::TimedRetryQueue->new( { name => q{mq_test_q1},
					   pop_timeout => 1,
					   max_retries => 6,
					 } );
my $mq2 = Redismq::TimedRetryQueue->new( { name => q{mq_test_q2},
					   pop_timeout => 1,
					   max_retries => 6,
					 } );
my $exec_action_fail_count = 1;
my $mover = Redismq::Mover->new( {
				  source_queue => $mq1,
				  destination_queue => $mq2,
				  exec_action
				  => sub {
				    if( $exec_action_fail_count-- ) {
				      diag q{exec_action - forced fail};
				      die;
				    } else {
				      diag qq{exec_action - ok with $_[0]};
				      uc( $_[0] )
				    } },
				 } );

my $message = q{abc};

#
# clean db and init of control vars
#
$mq1->redis->FLUSHALL();
is( $mq1->redis->keys( '*' ), 0, q{empty db} );
$test_count++;
my $q1_size = $mq1->size();
my $q2_size = $mq2->size();
#
# add message to the queue - push message
#
$mq1->push( $message );
is( $mq1->size(), ( $q1_size + 1 ), q{queue size} );
$test_count++;

#
# do a move - with fail
#
my $r = $mover->move();
is( $r, undef, qq{exec_action failed} );
is( $mq2->size(), $q2_size, qq{message ($message) not added to the destination_queue} );
$test_count += 2;

sleep(2);

#
# do move
#
my $r2 = $mover->move();
is( $r2, uc( $message ), qq{exec_action with $r2} );
is( $mq2->size(), $q2_size+1, qq{message ($message) added to the destination_queue} );
$test_count += 2;

done_testing( $test_count );
