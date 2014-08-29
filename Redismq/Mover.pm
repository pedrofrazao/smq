package Redismq::Mover;

use strict;
use warnings;

use Try::Tiny;
use Moo;

use Redismq::TimedRetryQueue;

has 'source_queue'
  => (
      is => 'ro',
      required => 1,
     );

has 'destination_queue'
  => (
      is => 'ro',
      required => 1,
     );

has 'exec_action'
  => (
      is => 'ro',
      required => 1,
      isa => sub {
	die qq{CODE expected as a exec_action\n}
	  unless ref $_[0] eq q{CODE};
      },
     );

#
# descr: move a message between the source queue and destination queue
#        executing some action in middle
#
# only 1 $m message will be pushed to the destination queue
#   (obtained by the sorted sets on Redis)
# the execution can be performed 1 or more times
# the $m message it's only removed after at least 1 execution
#
sub move {
  my $self = shift;
  my $m = $self->source_queue->pop();
  my $r;
  try {
    $r = $self->exec_action()->( $m );
  } catch {
    return;
  };
  if( $r ) {
    $self->destination_queue->push( $m );
    $self->source_queue->done( $m, $r );
    return $r;
  }
  return
}

1;
