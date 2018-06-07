#! /usr/bin/perl

use warnings;
use strict;

use Semja;

print "Running semja.\n";
Semja::initialize();
Semja::main(@ARGV);
print "Finished.\n";
