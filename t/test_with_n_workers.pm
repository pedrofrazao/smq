use test_with_n_workers;

use warnings;
use strict;

use Time::HiRes qw/time usleep/;

sub test {
  return;
}

sub run_with {
  my %args = @_;
  my $mq = $args{mq};
  my $test_count = 0;
  $args{sleep} ||= 1;

  my @pid = ();

  my $st = time;
  for( 1.. $args{n} ) {
    if( my $pid = fork ) {
      $mq->push( q{end_}.$pid );
      push(@pid, $pid);
    } else {
      # diag(qq{worker $$\n});
      while(1) {
	my $m = $mq->pop();
	if( $m ) {
	  usleep( $args{sleep} );
	  $mq->done( $m, q{ok} );
	  exit
	    if $m =~ m{^end_\d+$};
	} else {
	  sleep( 1 );
	}
      }
    }
  }

  is(scalar @pid, $args{n}, $args{n}.q{ workers} );
  $test_count++;

  my $et;
  while( my $p = pop(@pid) ) {
    waitpid( $p, 0 );
    $et = time;
  }

  diag(q{elapsed time: } . ( $et - $st ).q{ sec} );

  return $test_count;
}

1;
