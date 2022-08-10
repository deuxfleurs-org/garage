+++
title = "Failure & recovery"
weight = 50
+++

# Failure impact

Failures will lead to timeouts, which in turn could
lead to failed requests (this is a bug if failure enters in Garage tolerance)
and to increased latency as some retries might be performed.

How we proceed: we pause (`kill -STOP xxx`) one Garage process.
The idea is we don't want to close the TCP connection that would signal too easily
that a crash occured. Instead, we want to simulate a network error
or an overloaded process, ie. a 'non-collaborating' crash.


# Recovery impact
